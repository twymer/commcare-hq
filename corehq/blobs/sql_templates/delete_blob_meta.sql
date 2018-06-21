CREATE OR REPLACE FUNCTION delete_blob_meta(TEXT) RETURNS VOID AS $$
BEGIN
    DELETE FROM blobs_blobmeta WHERE path = $1;
END
$$ LANGUAGE plpgsql;
