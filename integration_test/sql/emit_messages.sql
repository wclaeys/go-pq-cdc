-- Requires wal_level=logical and pgoutput 'messages=on' on the client side.
CREATE OR REPLACE FUNCTION it_emit_msg() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  payload bytea;
BEGIN
  -- Build a compact JSON as bytea (example; could be protobuf etc.)
  payload := convert_to(
    jsonb_build_object(
      'op','u',
      'schema', TG_TABLE_SCHEMA,
      'table', TG_TABLE_NAME,
      'id', OLD.id,
      'old_status', OLD.status,
      'new_status', NEW.status
    )::text, 'UTF8');

  PERFORM pg_logical_emit_message(true, 'it.cdc', payload);
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS it_emit_msg_u ON public.users;
CREATE TRIGGER it_emit_msg_u
AFTER UPDATE ON public.users
FOR EACH ROW EXECUTE FUNCTION it_emit_msg();
