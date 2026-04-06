-- Purge consumed CDC rows older than 1 hour.
-- Execute per shadow table. Replace {schema} and {table} at runtime.
DELETE FROM "{schema}"."_EDELTA_CDC_{table}"
WHERE is_consumed = TRUE
  AND changed_at < ADD_SECONDS(CURRENT_TIMESTAMP, -3600);
