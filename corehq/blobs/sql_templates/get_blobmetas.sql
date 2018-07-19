DROP FUNCTION IF EXISTS get_blobmetas(TEXT[]);

CREATE FUNCTION get_blobmetas(parent_ids TEXT[]) RETURNS SETOF blobs_blobmeta AS $$
BEGIN
    -- order by parent id so that we don't have to do it in python
    RETURN QUERY
    SELECT * FROM blobs_blobmeta where parent_id = ANY(parent_ids) ORDER BY parent_ids;
END;
$$ LANGUAGE plpgsql;
