ALTER TABLE blobs_blobmeta RENAME TO blobs_blobmeta_tbl

CREATE OR REPLACE VIEW blobs_blobmeta (
    "id",
    "domain",
    "parent_id",
    "name",
    "path",
    "type_code",
    "content_length",
    "content_type",
    "properties",
    "created_on",
    "expires_on"
) AS
/*
 * Values in the "id" column are partitioned into three ranges:
 *
 * positive values: non-legacy blob metadata records
 * negative values: legacy form attachments
 *
 * Legacy metadata can be updated and deleted, but new records cannot be
 * inserted into these ranges. Some legacy metadata fields cannot be
 * updated: domain, type_code, created_on, expires_on.
 */

SELECT * FROM blobs_blobmeta_tbl

UNION ALL

SELECT
    -att."id" AS "id",
    xform."domain",
    att.form_id AS parent_id,
    att."name",
    COALESCE(COALESCE(att.blob_bucket, "form/" || att.attachment_id) || "/", "") || att.blob_id AS "path",
    CASE
        WHEN att."name" = "form.xml" THEN 1 -- corehq.blobs.CODES.form
        ELSE 2 -- corehq.blobs.CODES.form_attachment
    END CASE AS type_code,
    att.content_length,
    att.content_type,
    att.properties,
    xform.received_on AS created_on,
    NULL AS expires_on
FROM form_processor_xformattachmentsql att
    LEFT OUTER JOIN form_processor_xforminstancesql xform
        ON xform.form_id = att.form_id;


CREATE OR REPLACE FUNCTION mutate_blobs_blobmeta() RETURNS TRIGGER AS $$ BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO blobs_blobmeta_tbl (
            "id",
            "domain",
            "parent_id",
            "name",
            "path",
            "type_code",
            "content_length",
            "content_type",
            "properties",
            "created_on",
            "expires_on"
        ) VALUES (
            NEW."id",
            NEW."domain",
            NEW."parent_id",
            NEW."name",
            NEW."path",
            NEW."type_code",
            NEW."content_length",
            NEW."content_type",
            NEW."properties",
            NEW."created_on",
            NEW."expires_on"
        );
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD."id" >= 0 THEN
            UPDATE blobs_blobmeta_tbl SET
                "id" = NEW."id",
                "domain" = NEW."domain",
                "parent_id" = NEW."parent_id",
                "name" = NEW."name",
                "path" = NEW."path",
                "type_code" = NEW."type_code",
                "content_length" = NEW."content_length",
                "content_type" = NEW."content_type",
                "properties" = NEW."properties",
                "created_on" = NEW."created_on",
                "expires_on" = NEW."expires_on"
            WHERE OLD."id" >= 0 AND "id" = OLD."id";
        ELSE
            IF NEW.domain != OLD.domain THEN
                RAISE EXCEPTION 'Cannot change domain on attachment metadata';
            ELSIF NEW.type_code != OLD.type_code THEN
                RAISE EXCEPTION 'Cannot change type_code on attachment metadata';
            ELSIF NEW.created_on != OLD.created_on THEN
                RAISE EXCEPTION 'Cannot set created_on on attachment metadata';
            ELSIF NEW.expires_on IS NOT NULL THEN
                RAISE EXCEPTION 'Cannot set expires_on on attachment metadata';
            END IF;

            UPDATE form_processor_xformattachmentsql SET
                "form_id" = NEW."parent_id",
                "name" = NEW."name",
                "blob_bucket" = NULL,
                "attachment_id" = NULL,
                "blob_id" = NEW."path",
                "content_length" = NEW."content_length",
                "content_type" = NEW."content_type",
                "properties" = NEW."properties"
            WHERE "id" = -OLD."id";
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        IF OLD."id" >= 0 THEN
            DELETE FROM blobs_blobmeta_tbl
            WHERE "id" = OLD."id";
        ELSE
            DELETE FROM form_processor_xformattachmentsql
            WHERE "id" = -OLD."id";
        END IF;

        RETURN OLD;
    END IF;

    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER blobs_blobmeta_trigger
    INSTEAD OF INSERT OR UPDATE OR DELETE ON blobs_blobmeta
    FOR EACH ROW EXECUTE PROCEDURE mutate_blobs_blobmeta();
