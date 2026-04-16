import json
from typing import Any, Dict, Iterable, List, Optional

from psycopg import IntegrityError

from delivery_events import build_delivery_event_key, resolve_delivery_event_type
from release_versioning import extract_kinozal_id
from utils import compact_spaces, utc_ts

from .base import BaseRepository

DELIVERY_CLAIM_LEASE_SECONDS = 900
DELIVERY_QUEUE_LEASE_SECONDS = 300


def _load_delivery_audit(delivery_audit_json: Any) -> Dict[str, Any]:
    raw = str(delivery_audit_json or "").strip()
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _item_snapshot_from_audit(delivery_audit_json: Any) -> Dict[str, Any]:
    audit = _load_delivery_audit(delivery_audit_json)
    snapshot = audit.get("item_snapshot") if isinstance(audit, dict) else None
    return dict(snapshot) if isinstance(snapshot, dict) else {}


def _delivery_claim_ref_ts(row: Dict[str, Any]) -> int:
    updated_at = int(row.get("updated_at") or 0)
    claimed_at = int(row.get("claimed_at") or 0)
    return max(updated_at, claimed_at)


def _delivery_claim_is_active(row: Dict[str, Any], now: Optional[int] = None) -> bool:
    status = compact_spaces(str(row.get("status") or "")).lower()
    if status == "sent":
        return True
    if status != "sending":
        return False
    ref_ts = _delivery_claim_ref_ts(row)
    current_ts = int(now or utc_ts())
    return ref_ts > current_ts - DELIVERY_CLAIM_LEASE_SECONDS


class DeliveryRepository(BaseRepository):
    def delivery_event_persisted(
        self,
        tg_user_id: int,
        item_id: int,
        *,
        event_type: str = "",
        event_key: str = "",
    ) -> bool:
        resolved_event_type = compact_spaces(str(event_type or ""))
        resolved_event_key = compact_spaces(str(event_key or ""))
        if not resolved_event_type and not resolved_event_key:
            return self.delivered_persisted(tg_user_id, item_id)
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT delivery_audit_json
                FROM deliveries
                WHERE tg_user_id = ? AND item_id = ?
                UNION ALL
                SELECT delivery_audit_json
                FROM deliveries_archive
                WHERE tg_user_id = ? AND original_item_id = ?
                """,
                (tg_user_id, item_id, tg_user_id, item_id),
            ).fetchall()
        for row in rows:
            payload = dict(row)
            audit = _load_delivery_audit(payload.get("delivery_audit_json"))
            audit_event_key = compact_spaces(str(audit.get("event_key") or ""))
            audit_event_type = compact_spaces(str(audit.get("event_type") or ""))
            if resolved_event_key and audit_event_key == resolved_event_key:
                return True
            if not resolved_event_key and resolved_event_type and not audit_event_key and audit_event_type == resolved_event_type:
                return True
        return False

    def _delivery_claim_identity(
        self,
        tg_user_id: int,
        item_id: int,
        delivery_audit_json: str = "",
        *,
        context: str = "",
        event_type: str = "",
        event_key: str = "",
    ) -> Dict[str, Any]:
        snapshot = _item_snapshot_from_audit(delivery_audit_json)
        resolved_event_type = compact_spaces(event_type) or compact_spaces(str(_load_delivery_audit(delivery_audit_json).get("event_type") or "")) or resolve_delivery_event_type(context)
        resolved_event_key = compact_spaces(event_key) or compact_spaces(str(_load_delivery_audit(delivery_audit_json).get("event_key") or ""))
        if snapshot:
            kinozal_id = compact_spaces(str(snapshot.get("kinozal_id") or "")) or extract_kinozal_id(snapshot.get("source_uid")) or extract_kinozal_id(snapshot.get("source_link"))
            if not resolved_event_key:
                resolved_event_key = build_delivery_event_key(
                    tg_user_id,
                    snapshot,
                    context=context,
                    is_release_text_change=(resolved_event_type == "release_text"),
                )
            return {
                "kinozal_id": kinozal_id or None,
                "source_uid": compact_spaces(str(snapshot.get("source_uid") or "")) or None,
                "version_signature": compact_spaces(str(snapshot.get("version_signature") or "")) or None,
                "event_type": resolved_event_type or "release",
                "event_key": resolved_event_key,
            }
        payload = self.db.get_item_any(int(item_id)) if hasattr(self.db, "get_item_any") else None
        if not payload:
            return {"kinozal_id": None, "source_uid": None, "version_signature": None, "event_type": resolved_event_type or "release", "event_key": resolved_event_key}
        kinozal_id = compact_spaces(str(payload.get("kinozal_id") or "")) or extract_kinozal_id(payload.get("source_uid")) or extract_kinozal_id(payload.get("source_link"))
        if not resolved_event_key:
            resolved_event_key = build_delivery_event_key(
                tg_user_id,
                payload,
                context=context,
                is_release_text_change=(resolved_event_type == "release_text"),
            )
        return {
            "kinozal_id": kinozal_id or None,
            "source_uid": compact_spaces(str(payload.get("source_uid") or "")) or None,
            "version_signature": compact_spaces(str(payload.get("version_signature") or "")) or None,
            "event_type": resolved_event_type or "release",
            "event_key": resolved_event_key,
        }

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item_id: int,
        sub_id: Optional[int],
        matched_sub_ids: Optional[Iterable[int]] = None,
        delivery_audit: Optional[Dict[str, Any]] = None,
        context: str = "",
        event_type: str = "",
        event_key: str = "",
    ) -> bool:
        matched_ids_csv = None
        if matched_sub_ids:
            normalized_ids = sorted({int(x) for x in matched_sub_ids})
            matched_ids_csv = ",".join(str(x) for x in normalized_ids) if normalized_ids else None
        delivery_audit_json = json.dumps(delivery_audit, ensure_ascii=False, sort_keys=True) if delivery_audit else ""
        identity = self._delivery_claim_identity(
            tg_user_id,
            item_id,
            delivery_audit_json,
            context=context,
            event_type=event_type,
            event_key=event_key,
        )
        now = utc_ts()
        stale_cutoff = now - DELIVERY_CLAIM_LEASE_SECONDS
        with self.lock:
            try:
                row = self.conn.execute(
                    """
                    INSERT INTO delivery_claims(
                        tg_user_id, item_id, kinozal_id, source_uid, version_signature, event_type, event_key, subscription_id,
                        matched_subscription_ids, delivery_context, delivery_audit_json, status, last_error,
                        claimed_at, updated_at, sent_at
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sending', '', ?, ?, NULL)
                    ON CONFLICT (tg_user_id, event_key) DO UPDATE SET
                        item_id = EXCLUDED.item_id,
                        kinozal_id = EXCLUDED.kinozal_id,
                        source_uid = EXCLUDED.source_uid,
                        version_signature = EXCLUDED.version_signature,
                        event_type = EXCLUDED.event_type,
                        subscription_id = EXCLUDED.subscription_id,
                        matched_subscription_ids = EXCLUDED.matched_subscription_ids,
                        delivery_context = EXCLUDED.delivery_context,
                        delivery_audit_json = EXCLUDED.delivery_audit_json,
                        status = 'sending',
                        last_error = CASE
                            WHEN delivery_claims.status = 'sending'
                             AND COALESCE(delivery_claims.updated_at, delivery_claims.claimed_at) <= ?
                            THEN 'stale_claim_reclaimed'
                            ELSE ''
                        END,
                        claimed_at = EXCLUDED.claimed_at,
                        updated_at = EXCLUDED.updated_at,
                        sent_at = NULL
                    WHERE NOT (
                        delivery_claims.status = 'sent'
                        OR (
                            delivery_claims.status = 'sending'
                            AND COALESCE(delivery_claims.updated_at, delivery_claims.claimed_at) > ?
                        )
                    )
                    RETURNING tg_user_id
                    """,
                    (
                        tg_user_id,
                        item_id,
                        identity["kinozal_id"],
                        identity["source_uid"],
                        identity["version_signature"],
                        identity["event_type"],
                        identity["event_key"],
                        sub_id,
                        matched_ids_csv,
                        compact_spaces(context),
                        delivery_audit_json,
                        now,
                        now,
                        stale_cutoff,
                        stale_cutoff,
                    ),
                ).fetchone()
            except IntegrityError:
                row = None
            self.conn.commit()
        return row is not None

    def mark_delivery_claim_failed(
        self,
        tg_user_id: int,
        item_id: int,
        error: str = "",
        *,
        event_key: str = "",
    ) -> None:
        normalized_event_key = compact_spaces(str(event_key or ""))
        with self.lock:
            self.conn.execute(
                """
                UPDATE delivery_claims
                SET status = 'failed', last_error = ?, updated_at = ?
                WHERE tg_user_id = ?
                  AND status = 'sending'
                  AND (
                    (? <> '' AND COALESCE(event_key, '') = ?)
                    OR (? = '' AND item_id = ? AND COALESCE(event_key, '') = '')
                  )
                """,
                (
                    compact_spaces(error)[:500],
                    utc_ts(),
                    tg_user_id,
                    normalized_event_key,
                    normalized_event_key,
                    normalized_event_key,
                    item_id,
                ),
            )
            self.conn.commit()

    def _delivered_equivalent(self, tg_user_id: int, item: Dict[str, Any], include_claims: bool) -> bool:
        target_variant_sig = self.db.build_item_variant_signature(item) if hasattr(self.db, "build_item_variant_signature") else None
        if target_variant_sig is None:
            from release_versioning import build_item_variant_signature
            target_variant_sig = build_item_variant_signature(item)
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                rows = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchall()
                from release_versioning import build_item_variant_signature
                for row in rows:
                    if build_item_variant_signature(dict(row)) == target_variant_sig:
                        return True
                row = self.conn.execute(
                    """
                    SELECT 1
                    FROM deliveries_archive
                    WHERE tg_user_id = ?
                      AND kinozal_id = ?
                      AND COALESCE(version_signature, '') = ?
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id, compact_spaces(str(item.get("version_signature") or ""))),
                ).fetchone()
                if row is not None:
                    return True
                if include_claims:
                    row = self.conn.execute(
                        """
                        SELECT 1
                        FROM delivery_claims
                        WHERE tg_user_id = ?
                          AND kinozal_id = ?
                          AND COALESCE(version_signature, '') = ?
                          AND (
                            status = 'sent'
                            OR (status = 'sending' AND COALESCE(updated_at, claimed_at) > ?)
                          )
                        LIMIT 1
                        """,
                        (
                            tg_user_id,
                            kinozal_id,
                            compact_spaces(str(item.get("version_signature") or "")),
                            utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS,
                        ),
                    ).fetchone()
                    return row is not None
                return False

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return False
            rows = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                """,
                (tg_user_id, source_uid),
            ).fetchall()
            from release_versioning import build_item_variant_signature
            for row in rows:
                if build_item_variant_signature(dict(row)) == target_variant_sig:
                    return True
            row = self.conn.execute(
                """
                SELECT 1
                FROM deliveries_archive
                WHERE tg_user_id = ?
                  AND source_uid = ?
                  AND COALESCE(version_signature, '') = ?
                LIMIT 1
                """,
                (tg_user_id, source_uid, compact_spaces(str(item.get("version_signature") or ""))),
            ).fetchone()
            if row is not None:
                return True
            if include_claims:
                row = self.conn.execute(
                    """
                    SELECT 1
                    FROM delivery_claims
                    WHERE tg_user_id = ?
                      AND source_uid = ?
                      AND COALESCE(version_signature, '') = ?
                      AND (
                        status = 'sent'
                        OR (status = 'sending' AND COALESCE(updated_at, claimed_at) > ?)
                      )
                    LIMIT 1
                    """,
                    (
                        tg_user_id,
                        source_uid,
                        compact_spaces(str(item.get("version_signature") or "")),
                        utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS,
                    ),
                ).fetchone()
                return row is not None
            return False

    def delivered_equivalent(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        return self._delivered_equivalent(tg_user_id, item, include_claims=True)

    def delivered_equivalent_persisted(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        return self._delivered_equivalent(tg_user_id, item, include_claims=False)

    def get_latest_delivered_related_item(self, tg_user_id: int, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                row = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchone()
                if row:
                    return dict(row)
                archived = self.conn.execute(
                    """
                    SELECT da.delivery_audit_json, ia.item_json
                    FROM deliveries_archive da
                    JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                    WHERE da.tg_user_id = ?
                      AND da.kinozal_id = ?
                    ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id),
                ).fetchone()
                if archived:
                    snapshot = _item_snapshot_from_audit(archived.get("delivery_audit_json"))
                    fallback_payload: Dict[str, Any] = {}
                    if archived.get("item_json"):
                        try:
                            fallback_payload = json.loads(archived["item_json"])
                        except Exception:
                            fallback_payload = {}
                    if snapshot:
                        merged = dict(fallback_payload)
                        merged.update({k: v for k, v in snapshot.items() if v not in (None, "")})
                        if merged:
                            return merged
                if archived and archived.get("item_json"):
                    try:
                        return json.loads(archived["item_json"])
                    except Exception:
                        pass
                return None

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return None
            row = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if row:
                return dict(row)
            archived = self.conn.execute(
                """
                SELECT da.delivery_audit_json, ia.item_json
                FROM deliveries_archive da
                JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                WHERE da.tg_user_id = ?
                  AND da.source_uid = ?
                ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if archived:
                snapshot = _item_snapshot_from_audit(archived.get("delivery_audit_json"))
                fallback_payload: Dict[str, Any] = {}
                if archived.get("item_json"):
                    try:
                        fallback_payload = json.loads(archived["item_json"])
                    except Exception:
                        fallback_payload = {}
                if snapshot:
                    merged = dict(fallback_payload)
                    merged.update({k: v for k, v in snapshot.items() if v not in (None, "")})
                    if merged:
                        return merged
            if archived and archived.get("item_json"):
                try:
                    return json.loads(archived["item_json"])
                except Exception:
                    pass
            return None

    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT d.tg_user_id, d.item_id AS delivered_item_id
                    FROM deliveries d
                    UNION ALL
                    SELECT da.tg_user_id, da.original_item_id AS delivered_item_id
                    FROM deliveries_archive da
                    UNION ALL
                    SELECT dc.tg_user_id, dc.item_id AS delivered_item_id
                    FROM delivery_claims dc
                    WHERE dc.status = 'sent'
                       OR (dc.status = 'sending' AND COALESCE(dc.updated_at, dc.claimed_at) > ?)
                ) delivered_rows
                WHERE tg_user_id = ? AND delivered_item_id = ?
                LIMIT 1
                """,
                (utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS, tg_user_id, item_id),
            ).fetchone()
            return row is not None

    def delivered_persisted(self, tg_user_id: int, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT d.tg_user_id, d.item_id AS delivered_item_id
                    FROM deliveries d
                    UNION ALL
                    SELECT da.tg_user_id, da.original_item_id AS delivered_item_id
                    FROM deliveries_archive da
                ) delivered_rows
                WHERE tg_user_id = ? AND delivered_item_id = ?
                LIMIT 1
                """,
                (tg_user_id, item_id),
            ).fetchone()
            return row is not None

    def record_delivery(
        self,
        tg_user_id: int,
        item_id: int,
        sub_id: Optional[int],
        matched_sub_ids: Optional[Iterable[int]] = None,
        delivery_audit: Optional[Dict[str, Any]] = None,
        *,
        event_type: str = "",
        event_key: str = "",
    ) -> None:
        matched_ids_csv = None
        if matched_sub_ids:
            normalized_ids = sorted({int(x) for x in matched_sub_ids})
            matched_ids_csv = ",".join(str(x) for x in normalized_ids) if normalized_ids else None
        delivery_audit_json = json.dumps(delivery_audit, ensure_ascii=False, sort_keys=True) if delivery_audit else ""
        audit = _load_delivery_audit(delivery_audit_json)
        resolved_event_type = compact_spaces(str(event_type or audit.get("event_type") or ""))
        resolved_event_key = compact_spaces(str(event_key or audit.get("event_key") or ""))
        with self.lock:
            live_item = self.conn.execute(
                "SELECT 1 FROM items WHERE id = ? LIMIT 1",
                (item_id,),
            ).fetchone()
            if not live_item:
                archived_item = self.conn.execute(
                    """
                    SELECT *
                    FROM items_archive
                    WHERE original_item_id = ?
                    ORDER BY archived_at DESC, archive_id DESC
                    LIMIT 1
                    """,
                    (item_id,),
                ).fetchone()
                if not archived_item:
                    return
                archived_payload = dict(archived_item)
                snapshot = _item_snapshot_from_audit(delivery_audit_json)
                delivered_at = utc_ts()
                if not self.delivery_event_persisted(
                    tg_user_id,
                    item_id,
                    event_type=resolved_event_type,
                    event_key=resolved_event_key,
                ):
                    self.conn.execute(
                        """
                        INSERT INTO deliveries_archive(
                            original_delivery_id, tg_user_id, original_item_id, kinozal_id, source_uid, media_type,
                            version_signature, source_title, subscription_id, matched_subscription_ids,
                            delivery_audit_json, delivered_at, archived_at, archive_reason, merged_into_item_id
                        )
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            None,
                            tg_user_id,
                            item_id,
                            compact_spaces(str(snapshot.get("kinozal_id") or archived_payload.get("kinozal_id") or "")) or None,
                            snapshot.get("source_uid") or archived_payload.get("source_uid"),
                            snapshot.get("media_type") or archived_payload.get("media_type"),
                            snapshot.get("version_signature") or archived_payload.get("version_signature"),
                            snapshot.get("source_title") or archived_payload.get("source_title"),
                            sub_id,
                            matched_ids_csv,
                            delivery_audit_json,
                            delivered_at,
                            delivered_at,
                            "delivered_from_archive",
                            archived_payload.get("merged_into_item_id"),
                        ),
                    )
                self.conn.execute(
                    """
                    UPDATE delivery_claims
                    SET status = 'sent', last_error = '', sent_at = ?, updated_at = ?
                    WHERE tg_user_id = ?
                      AND (
                        (? <> '' AND COALESCE(event_key, '') = ?)
                        OR (
                            ? = ''
                            AND item_id = ?
                            AND COALESCE(event_type, '') = ?
                            AND COALESCE(event_key, '') = ''
                        )
                    )
                    """,
                    (
                        delivered_at,
                        delivered_at,
                        tg_user_id,
                        resolved_event_key,
                        resolved_event_key,
                        resolved_event_key,
                        item_id,
                        resolved_event_type,
                    ),
                )
                self.conn.commit()
                return
            self.conn.execute(
                """
                INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivery_audit_json, delivered_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_user_id, item_id) DO NOTHING
                """,
                (tg_user_id, item_id, sub_id, matched_ids_csv, delivery_audit_json, utc_ts()),
            )
            sent_at = utc_ts()
            self.conn.execute(
                """
                UPDATE delivery_claims
                SET status = 'sent', last_error = '', sent_at = ?, updated_at = ?
                WHERE tg_user_id = ?
                  AND (
                    (? <> '' AND COALESCE(event_key, '') = ?)
                    OR (
                        ? = ''
                        AND item_id = ?
                        AND COALESCE(event_type, '') = ?
                        AND COALESCE(event_key, '') = ''
                    )
                  )
                """,
                (
                    sent_at,
                    sent_at,
                    tg_user_id,
                    resolved_event_key,
                    resolved_event_key,
                    resolved_event_key,
                    item_id,
                    resolved_event_type,
                ),
            )
            self.conn.commit()

    def get_delivery_audits(
        self,
        kinozal_id: str,
        tg_user_id: Optional[int] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return []
        params: List[Any] = [kinozal_id, kinozal_id]
        sql = """
            SELECT *
            FROM (
                SELECT d.tg_user_id, d.item_id, d.subscription_id, d.matched_subscription_ids,
                       d.delivery_audit_json, d.delivered_at, s.name AS subscription_name,
                       'live' AS delivery_source
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                LEFT JOIN subscriptions s ON s.id = d.subscription_id
                WHERE i.kinozal_id = ?

                UNION ALL

                SELECT da.tg_user_id, da.original_item_id AS item_id, da.subscription_id, da.matched_subscription_ids,
                       da.delivery_audit_json, da.delivered_at, s.name AS subscription_name,
                       'archive' AS delivery_source
                FROM deliveries_archive da
                LEFT JOIN subscriptions s ON s.id = da.subscription_id
                WHERE da.kinozal_id = ?
            ) audit_rows
            WHERE 1 = 1
        """
        if tg_user_id is not None:
            sql += " AND tg_user_id = ?"
            params.append(int(tg_user_id))
        sql += " ORDER BY delivered_at DESC, item_id DESC LIMIT ?"
        params.append(max(1, min(int(limit or 10), 20)))
        with self.lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            audit_json = compact_spaces(str(data.get("delivery_audit_json") or ""))
            if audit_json:
                try:
                    data["delivery_audit"] = json.loads(audit_json)
                except Exception:
                    data["delivery_audit"] = {}
            else:
                data["delivery_audit"] = {}
            result.append(data)
        return result

    def recently_delivered(self, tg_user_id: int, item_id: int, cooldown_seconds: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT d.tg_user_id, d.item_id AS delivered_item_id, d.delivered_at
                    FROM deliveries d
                    UNION ALL
                    SELECT da.tg_user_id, da.original_item_id AS delivered_item_id, da.delivered_at
                    FROM deliveries_archive da
                    UNION ALL
                    SELECT dc.tg_user_id, dc.item_id AS delivered_item_id, COALESCE(dc.sent_at, dc.claimed_at) AS delivered_at
                    FROM delivery_claims dc
                    WHERE dc.status = 'sent'
                       OR (dc.status = 'sending' AND COALESCE(dc.updated_at, dc.claimed_at) > ?)
                ) delivered_rows
                WHERE tg_user_id = ? AND delivered_item_id = ? AND delivered_at > ?
                LIMIT 1
                """,
                (utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS, tg_user_id, item_id, utc_ts() - cooldown_seconds),
            ).fetchone()
            return row is not None

    def upsert_debounce(
        self,
        tg_user_id: int,
        kinozal_id: str,
        item_id: int,
        matched_sub_ids: str,
        delay_seconds: int,
        event_key: str = "",
    ) -> None:
        after_ts = utc_ts() + delay_seconds
        with self.lock:
            self.conn.execute(
                """INSERT INTO debounce_queue (tg_user_id, kinozal_id, item_id, matched_sub_ids, deliver_after_ts, reset_count, event_key)
                   VALUES (?, ?, ?, ?, ?, 0, ?)
                   ON CONFLICT (tg_user_id, kinozal_id) DO UPDATE SET
                       item_id = excluded.item_id,
                       matched_sub_ids = excluded.matched_sub_ids,
                       event_key = excluded.event_key,
                       deliver_after_ts = CASE
                           WHEN debounce_queue.reset_count < 2 THEN excluded.deliver_after_ts
                           ELSE debounce_queue.deliver_after_ts
                       END,
                       reset_count = CASE
                           WHEN debounce_queue.reset_count < 2 THEN debounce_queue.reset_count + 1
                           ELSE debounce_queue.reset_count
                       END""",
                (tg_user_id, kinozal_id, item_id, matched_sub_ids or "", after_ts, compact_spaces(event_key) or f"debounce:{tg_user_id}:{compact_spaces(str(kinozal_id or ''))}"),
            )
            self.conn.commit()

    def pop_due_debounce(self) -> List[Dict[str, Any]]:
        now = utc_ts()
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM debounce_queue WHERE deliver_after_ts <= ? ORDER BY deliver_after_ts ASC",
                (now,),
            ).fetchall()
            return [dict(row) for row in rows]

    def lease_due_debounce_entries(
        self,
        current_ts: Optional[int] = None,
        *,
        lease_seconds: int = DELIVERY_QUEUE_LEASE_SECONDS,
    ) -> List[Dict[str, Any]]:
        now = int(current_ts or utc_ts())
        lease_token = f"debounce-{now}"
        lease_expires_at = now + int(lease_seconds)
        with self.lock:
            rows = self.conn.execute(
                """
                UPDATE debounce_queue dq
                SET lease_token = ?,
                    lease_expires_at = ?,
                    attempt_count = COALESCE(dq.attempt_count, 0) + 1
                FROM (
                    SELECT tg_user_id, kinozal_id
                    FROM debounce_queue
                    WHERE deliver_after_ts <= ?
                      AND COALESCE(lease_expires_at, 0) <= ?
                    ORDER BY deliver_after_ts ASC
                    FOR UPDATE SKIP LOCKED
                ) picked
                WHERE dq.tg_user_id = picked.tg_user_id
                  AND dq.kinozal_id = picked.kinozal_id
                RETURNING dq.*
                """,
                (lease_token, lease_expires_at, now, now),
            ).fetchall()
            self.conn.commit()
        return [dict(row) for row in rows]

    def release_debounce_lease(
        self,
        tg_user_id: int,
        kinozal_id: str,
        *,
        lease_token: str,
        error: str = "",
        deliver_after_ts: Optional[int] = None,
    ) -> None:
        assignments = [
            "lease_token = ''",
            "lease_expires_at = 0",
            "last_error = ?",
        ]
        params: List[Any] = [compact_spaces(error)[:500]]
        if deliver_after_ts is not None:
            assignments.append("deliver_after_ts = ?")
            params.append(int(deliver_after_ts))
        params.extend([int(tg_user_id), compact_spaces(str(kinozal_id or "")), compact_spaces(str(lease_token or ""))])
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE debounce_queue
                SET {', '.join(assignments)}
                WHERE tg_user_id = ?
                  AND kinozal_id = ?
                  AND COALESCE(lease_token, '') = ?
                """,
                tuple(params),
            )
            self.conn.commit()

    def delete_debounce_entry(self, tg_user_id: int, kinozal_id: str) -> None:
        with self.lock:
            self.conn.execute(
                "DELETE FROM debounce_queue WHERE tg_user_id = ? AND kinozal_id = ?",
                (tg_user_id, compact_spaces(str(kinozal_id or ""))),
            )
            self.conn.commit()

    def recently_delivered_kinozal_id(self, tg_user_id: int, kinozal_id: str, cooldown_seconds: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT d.tg_user_id, i.kinozal_id, d.delivered_at
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    UNION ALL
                    SELECT da.tg_user_id, da.kinozal_id, da.delivered_at
                    FROM deliveries_archive da
                    UNION ALL
                    SELECT dc.tg_user_id, dc.kinozal_id, COALESCE(dc.sent_at, dc.claimed_at) AS delivered_at
                    FROM delivery_claims dc
                    WHERE dc.status = 'sent'
                       OR (dc.status = 'sending' AND COALESCE(dc.updated_at, dc.claimed_at) > ?)
                ) delivered_rows
                WHERE tg_user_id = ? AND kinozal_id = ? AND delivered_at > ?
                LIMIT 1
                """,
                (utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS, tg_user_id, kinozal_id, utc_ts() - cooldown_seconds),
            ).fetchone()
            return row is not None

    def was_delivered_to_anyone(self, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM (
                    SELECT d.item_id
                    FROM deliveries d
                    WHERE d.item_id = ?
                    UNION ALL
                    SELECT da.original_item_id AS item_id
                    FROM deliveries_archive da
                    WHERE da.original_item_id = ?
                    UNION ALL
                    SELECT dc.item_id
                    FROM delivery_claims dc
                    WHERE dc.item_id = ?
                      AND (
                        dc.status = 'sent'
                        OR (dc.status = 'sending' AND COALESCE(dc.updated_at, dc.claimed_at) > ?)
                      )
                ) delivered_rows
                LIMIT 1
                """,
                (item_id, item_id, item_id, utc_ts() - DELIVERY_CLAIM_LEASE_SECONDS),
            ).fetchone()
            return row is not None

    def mute_title(self, tg_user_id: int, tmdb_id: int) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                "INSERT INTO muted_titles (tg_user_id, tmdb_id, created_at) VALUES (?, ?, ?) ON CONFLICT (tg_user_id, tmdb_id) DO NOTHING",
                (tg_user_id, tmdb_id, ts),
            )
            self.conn.commit()

    def unmute_title(self, tg_user_id: int, tmdb_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "DELETE FROM muted_titles WHERE tg_user_id = ? AND tmdb_id = ?",
                (tg_user_id, tmdb_id),
            )
            self.conn.commit()

    def is_title_muted(self, tg_user_id: int, tmdb_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM muted_titles WHERE tg_user_id = ? AND tmdb_id = ? LIMIT 1",
                (tg_user_id, tmdb_id),
            ).fetchone()
            return row is not None

    def list_muted_titles(self, tg_user_id: int, limit: int = 30) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """SELECT mt.tmdb_id,
                          COALESCE(
                              (SELECT i.tmdb_title FROM items i
                               WHERE i.tmdb_id = mt.tmdb_id AND i.tmdb_title IS NOT NULL
                               ORDER BY i.id DESC LIMIT 1),
                              (SELECT i.source_title FROM items i
                               WHERE i.tmdb_id = mt.tmdb_id
                               ORDER BY i.id DESC LIMIT 1)
                          ) AS title,
                          (SELECT i.media_type FROM items i
                           WHERE i.tmdb_id = mt.tmdb_id
                           ORDER BY i.id DESC LIMIT 1) AS media_type,
                          mt.created_at
                   FROM muted_titles mt
                   WHERE mt.tg_user_id = ?
                   ORDER BY mt.created_at DESC
                   LIMIT ?""",
                (tg_user_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_user_delivery_history(self, tg_user_id: int, limit: int = 15) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT *
                FROM (
                    SELECT
                        i.id,
                        i.source_title,
                        i.source_link,
                        i.tmdb_title,
                        i.media_type,
                        d.delivered_at
                    FROM deliveries d
                    JOIN items i ON d.item_id = i.id
                    WHERE d.tg_user_id = ?

                    UNION ALL

                    SELECT
                        da.original_item_id AS id,
                        da.source_title,
                        ia.source_link,
                        ia.tmdb_title,
                        da.media_type,
                        da.delivered_at
                    FROM deliveries_archive da
                    LEFT JOIN items_archive ia
                      ON ia.original_item_id = da.original_item_id
                    WHERE da.tg_user_id = ?
                ) history_rows
                ORDER BY delivered_at DESC
                LIMIT ?
                """,
                (tg_user_id, tg_user_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def queue_pending_delivery(
        self,
        tg_user_id: int,
        item_id: int,
        matched_sub_ids: str,
        old_release_text: str,
        is_release_text_change: bool,
        *,
        event_type: str = "",
        event_key: str = "",
        deliver_not_before_ts: Optional[int] = None,
    ) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """INSERT INTO pending_deliveries
                   (tg_user_id, item_id, matched_sub_ids, old_release_text, is_release_text_change, queued_at,
                    event_type, event_key, deliver_not_before_ts, lease_token, lease_expires_at, attempt_count, last_error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, 0, '')
                   ON CONFLICT (tg_user_id, event_key) DO UPDATE SET
                       item_id = excluded.item_id,
                       matched_sub_ids = excluded.matched_sub_ids,
                       old_release_text = excluded.old_release_text,
                       is_release_text_change = excluded.is_release_text_change,
                       queued_at = excluded.queued_at,
                       event_type = excluded.event_type,
                       deliver_not_before_ts = excluded.deliver_not_before_ts,
                       lease_token = '',
                       lease_expires_at = 0,
                       last_error = ''""",
                (
                    tg_user_id,
                    item_id,
                    matched_sub_ids or "",
                    old_release_text or "",
                    1 if is_release_text_change else 0,
                    ts,
                    compact_spaces(event_type) or ("release_text" if is_release_text_change else "release"),
                    compact_spaces(event_key) or f"pending:{tg_user_id}:{item_id}:{'release_text' if is_release_text_change else 'release'}",
                    int(deliver_not_before_ts or ts),
                ),
            )
            self.conn.commit()

    def lease_due_pending_deliveries(
        self,
        current_ts: Optional[int] = None,
        *,
        lease_seconds: int = DELIVERY_QUEUE_LEASE_SECONDS,
    ) -> List[Dict[str, Any]]:
        now = int(current_ts or utc_ts())
        lease_token = f"pending-{now}"
        lease_expires_at = now + int(lease_seconds)
        with self.lock:
            rows = self.conn.execute(
                """
                UPDATE pending_deliveries pd
                SET lease_token = ?,
                    lease_expires_at = ?,
                    attempt_count = COALESCE(pd.attempt_count, 0) + 1
                FROM (
                    SELECT id
                    FROM pending_deliveries
                    WHERE COALESCE(deliver_not_before_ts, queued_at) <= ?
                      AND COALESCE(lease_expires_at, 0) <= ?
                    ORDER BY queued_at ASC
                    FOR UPDATE SKIP LOCKED
                ) picked
                WHERE pd.id = picked.id
                RETURNING pd.*
                """,
                (lease_token, lease_expires_at, now, now),
            ).fetchall()
            self.conn.commit()
        return [dict(row) for row in rows]

    def release_pending_delivery_lease(
        self,
        pending_id: int,
        *,
        lease_token: str,
        error: str = "",
        deliver_not_before_ts: Optional[int] = None,
    ) -> None:
        assignments = [
            "lease_token = ''",
            "lease_expires_at = 0",
            "last_error = ?",
        ]
        params: List[Any] = [compact_spaces(error)[:500]]
        if deliver_not_before_ts is not None:
            assignments.append("deliver_not_before_ts = ?")
            params.append(int(deliver_not_before_ts))
        params.extend([int(pending_id), compact_spaces(str(lease_token or ""))])
        with self.lock:
            self.conn.execute(
                f"""
                UPDATE pending_deliveries
                SET {', '.join(assignments)}
                WHERE id = ?
                  AND COALESCE(lease_token, '') = ?
                """,
                tuple(params),
            )
            self.conn.commit()

    def pop_due_pending_deliveries(self, current_hour: int) -> Dict[int, List[Dict[str, Any]]]:
        with self.lock:
            rows = self.conn.execute(
                """SELECT pd.tg_user_id, pd.item_id, pd.matched_sub_ids, pd.old_release_text,
                          pd.is_release_text_change, u.quiet_start_hour, u.quiet_end_hour
                   FROM pending_deliveries pd
                   JOIN users u ON pd.tg_user_id = u.tg_user_id
                   ORDER BY pd.queued_at ASC""",
            ).fetchall()
        result: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            pending = dict(row)
            start_h = pending.get("quiet_start_hour")
            end_h = pending.get("quiet_end_hour")
            if start_h is not None and end_h is not None:
                if start_h < end_h:
                    still_quiet = start_h <= current_hour < end_h
                else:
                    still_quiet = current_hour >= start_h or current_hour < end_h
                if still_quiet:
                    continue
            result.setdefault(pending["tg_user_id"], []).append(pending)
        return result

    def delete_pending_delivery(self, tg_user_id: int, item_id: int, event_key: str = "") -> None:
        with self.lock:
            if compact_spaces(event_key):
                self.conn.execute(
                    "DELETE FROM pending_deliveries WHERE tg_user_id = ? AND item_id = ? AND event_key = ?",
                    (tg_user_id, item_id, compact_spaces(event_key)),
                )
            else:
                self.conn.execute(
                    "DELETE FROM pending_deliveries WHERE tg_user_id = ? AND item_id = ?",
                    (tg_user_id, item_id),
                )
            self.conn.commit()
