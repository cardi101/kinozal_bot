import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fmt_ts(value: int) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")


def _csv_ints(value: Any) -> List[int]:
    return [int(part) for part in str(value or "").split(",") if part.strip().isdigit()]


def _fetch_bad_targets(dsn: str, kinozal_id: str = "") -> List[Dict[str, Any]]:
    params: List[Any] = []
    kinozal_filter = ""
    if kinozal_id:
        kinozal_filter = "AND dc.kinozal_id = %s"
        params.append(kinozal_id)

    with connect(dsn, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH bad_observations AS (
                    SELECT DISTINCT kinozal_id, release_text_hash
                    FROM source_observations
                    WHERE source_kind = 'details'
                      AND COALESCE(raw_payload_json->>'source_release_text', '') LIKE '%%пїЅ%%'
                      AND COALESCE(release_text_hash, '') <> ''
                ),
                bad_claims AS (
                    SELECT DISTINCT ON (dc.kinozal_id, dc.tg_user_id)
                           dc.kinozal_id,
                           dc.tg_user_id,
                           dc.item_id AS bad_item_id,
                           dc.subscription_id,
                           dc.matched_subscription_ids,
                           dc.event_key AS bad_event_key,
                           dc.sent_at AS bad_sent_at
                    FROM delivery_claims dc
                    JOIN bad_observations bo
                      ON bo.kinozal_id = dc.kinozal_id
                     AND bo.release_text_hash = split_part(dc.event_key, ':', 4)
                    WHERE dc.event_type = 'release_text'
                      AND dc.status = 'sent'
                      {kinozal_filter}
                    ORDER BY dc.kinozal_id, dc.tg_user_id, dc.sent_at DESC
                )
                SELECT bc.*,
                       to_jsonb(i) AS item_payload
                FROM bad_claims bc
                JOIN LATERAL (
                    SELECT *
                    FROM items i
                    WHERE i.kinozal_id = bc.kinozal_id
                    ORDER BY i.id DESC
                    LIMIT 1
                ) i ON true
                WHERE COALESCE(i.source_release_text, '') <> ''
                  AND i.source_release_text NOT LIKE '%%пїЅ%%'
                ORDER BY bc.bad_sent_at DESC, bc.kinozal_id, bc.tg_user_id
                """,
                params,
            )
            return list(cur.fetchall())


def _fetch_subscriptions(db: Any, sub_ids: List[int]) -> List[Dict[str, Any]]:
    if not sub_ids:
        return []
    rows = db.conn.execute(
        "SELECT * FROM subscriptions WHERE id = ANY(%s) ORDER BY id",
        (sub_ids,),
    ).fetchall()
    return [dict(row) for row in rows]


async def _run() -> None:
    from aiogram import Bot
    from aiogram.enums import ParseMode

    from config import CFG
    from db import DB
    from delivery_audit import build_delivery_audit
    from delivery_events import build_delivery_event_key
    from delivery_sender import _build_release_followup_messages, _tg_retry

    load_dotenv()
    parser = argparse.ArgumentParser(description="Preview or send corrected release-text notifications after mojibake sends")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", ""), help="Postgres DSN, defaults to DATABASE_URL")
    parser.add_argument("--kinozal-id", default="", help="Limit to one kinozal_id")
    parser.add_argument("--limit", type=int, default=100, help="Maximum user targets to process")
    parser.add_argument("--apply", action="store_true", help="Send correction notifications and record release_text claims")
    args = parser.parse_args()

    if not args.dsn:
        raise RuntimeError("DATABASE_URL is required")

    db = DB(args.dsn)
    bot = Bot(CFG.bot_token) if args.apply else None
    rows = _fetch_bad_targets(args.dsn, kinozal_id=args.kinozal_id.strip())
    results: List[Dict[str, Any]] = []

    try:
        for row in rows:
            if len(results) >= max(int(args.limit or 0), 0):
                break
            item = dict(row["item_payload"] or {})
            tg_user_id = int(row["tg_user_id"])
            kinozal_id = str(row["kinozal_id"])
            matched_ids = _csv_ints(row.get("matched_subscription_ids"))
            if not matched_ids and row.get("subscription_id"):
                matched_ids = [int(row["subscription_id"])]
            sub_id = matched_ids[0] if matched_ids else None
            subs = _fetch_subscriptions(db, matched_ids)
            event_key = build_delivery_event_key(
                tg_user_id,
                item,
                context="release_text_update",
                is_release_text_change=True,
                release_text=str(item.get("source_release_text") or ""),
            )
            existing = db.conn.execute(
                """
                SELECT status
                FROM delivery_claims
                WHERE tg_user_id = ?
                  AND event_key = ?
                  AND status = 'sent'
                LIMIT 1
                """,
                (tg_user_id, event_key),
            ).fetchone()
            messages = _build_release_followup_messages(item, old_release_text="")
            result: Dict[str, Any] = {
                "kinozal_id": kinozal_id,
                "tg_user_id": tg_user_id,
                "item_id": int(item.get("id") or 0),
                "bad_sent_at": int(row.get("bad_sent_at") or 0),
                "bad_event_key": str(row.get("bad_event_key") or ""),
                "repair_event_key": event_key,
                "messages": len(messages),
                "status": "ready",
                "reason": "",
                "title": str(item.get("source_title") or "")[:120],
            }
            if existing:
                result["status"] = "skipped"
                result["reason"] = "repair_claim_already_sent"
            elif not messages:
                result["status"] = "skipped"
                result["reason"] = "no_release_text_message"
            elif args.apply:
                audit = build_delivery_audit(db, item, subs, context="mojibake_release_text_repair")
                audit["event_type"] = "release_text"
                audit["event_key"] = event_key
                if not db.begin_delivery_claim(
                    tg_user_id,
                    int(item.get("id") or 0),
                    sub_id,
                    matched_ids,
                    delivery_audit=audit,
                    context="mojibake_release_text_repair",
                    event_type="release_text",
                    event_key=event_key,
                ):
                    result["status"] = "skipped"
                    result["reason"] = "claim_exists_or_in_flight"
                else:
                    try:
                        for text in messages:
                            await _tg_retry(
                                bot.send_message,
                                tg_user_id,
                                text=text,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                        db.record_delivery(
                            tg_user_id,
                            int(item.get("id") or 0),
                            sub_id,
                            matched_ids,
                            delivery_audit=audit,
                            event_type="release_text",
                            event_key=event_key,
                        )
                        result["status"] = "sent"
                    except Exception as exc:
                        db.mark_delivery_claim_failed(
                            tg_user_id,
                            int(item.get("id") or 0),
                            error=str(exc),
                            event_key=event_key,
                        )
                        result["status"] = "failed"
                        result["reason"] = str(exc)[:200]
            results.append(result)
    finally:
        if bot is not None:
            await bot.session.close()
        db.close()

    status_counts: Dict[str, int] = {}
    for result in results:
        status_counts[result["status"]] = status_counts.get(result["status"], 0) + 1
        print(
            " | ".join(
                [
                    f"status={result['status']}",
                    f"reason={result['reason']}",
                    f"kinozal_id={result['kinozal_id']}",
                    f"user={result['tg_user_id']}",
                    f"item={result['item_id']}",
                    f"bad_sent_at={_fmt_ts(result['bad_sent_at'])}",
                    f"messages={result['messages']}",
                    f"title={result['title']}",
                ]
            )
        )
    print(f"mode={'apply' if args.apply else 'dry-run'} | targets={len(results)} | status_counts={status_counts}")


if __name__ == "__main__":
    asyncio.run(_run())
