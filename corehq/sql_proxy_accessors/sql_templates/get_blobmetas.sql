DROP FUNCTION IF EXISTS get_blobmetas(TEXT[]);

CREATE FUNCTION get_blobmetas(parent_ids TEXT[]) RETURNS SETOF blobs_blobmeta AS $$
    CLUSTER '{{ PL_PROXY_CLUSTER_NAME }}';
    SPLIT parent_ids;
    RUN ON hash_string(parent_ids, 'siphash24');
$$ LANGUAGE plproxy;
