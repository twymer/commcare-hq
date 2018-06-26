from __future__ import absolute_import
from __future__ import unicode_literals
from corehq.blobs.exceptions import InvalidContext


class AtomicBlobs(object):
    """A blob db wrapper that can put and delete blobs atomically

    Usage:

        with AtomicBlobs(get_blob_db()) as db:
            # do stuff here that puts or deletes blobs
            db.delete(old_blob_id)
            meta = db.put(content, new_blob_id)
            save(meta, deleted=old_blob_id)

    If an exception occurs inside the `AtomicBlobs` context then all
    blob write operations (puts and deletes) will be rolled back.
    """

    def __init__(self, db):
        self.db = db
        self.puts = None
        self.deletes = None

    def put(self, *args, **kw):
        if self.puts is None:
            raise InvalidContext("AtomicBlobs context is not active")
        meta = self.db.put(*args, **kw)
        self.puts.append(meta)
        return meta

    def get(self, *args, **kw):
        return self.db.get(*args, **kw)

    def delete(self, path):
        """Delete a blob

        NOTE blobs will not actually be deleted until the context exits,
        so subsequent gets inside the context will return an object even
        though the blob or bucket has been queued for deletion.
        """
        if self.puts is None:
            raise InvalidContext("AtomicBlobs context is not active")
        self.deletes.append(path)
        return None  # result is unknown

    def copy_blob(self, *args, **kw):
        raise NotImplementedError

    def __enter__(self):
        self.puts = []
        self.deletes = []
        return self

    def __exit__(self, exc_type, exc_value, tb):
        puts, deletes = self.puts, self.deletes
        self.puts = None
        self.deletes = None
        if exc_type is None:
            for path in deletes:
                self.db.delete(path)
        else:
            self.db.bulk_delete(puts)
