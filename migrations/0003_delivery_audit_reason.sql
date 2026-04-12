ALTER TABLE deliveries
ADD COLUMN IF NOT EXISTS delivery_audit_json TEXT NOT NULL DEFAULT '';

ALTER TABLE deliveries_archive
ADD COLUMN IF NOT EXISTS delivery_audit_json TEXT NOT NULL DEFAULT '';
