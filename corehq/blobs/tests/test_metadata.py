from __future__ import unicode_literals
from __future__ import absolute_import
from io import BytesIO
from shutil import rmtree
from tempfile import mkdtemp

from django.db import connections
from django.test import TestCase

import corehq.blobs.fsdb as fsdb
from corehq.blobs import CODES
from corehq.blobs.models import BlobMeta
from corehq.blobs.tests.util import get_meta, new_meta
from corehq.sql_db.routers import db_for_read_write


class TestMetaDB(TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestMetaDB, cls).setUpClass()
        cls.rootdir = mkdtemp(prefix="blobdb")
        cls.db = fsdb.FilesystemBlobDB(cls.rootdir)

    @classmethod
    def tearDownClass(cls):
        cls.db = None
        rmtree(cls.rootdir)
        cls.rootdir = None
        super(TestMetaDB, cls).tearDownClass()

    def test_new(self):
        metadb = self.db.metadb
        with self.assertRaisesMessage(TypeError, "domain is required"):
            metadb.new()
        with self.assertRaisesMessage(TypeError, "parent_id is required"):
            metadb.new(domain="test")
        with self.assertRaisesMessage(TypeError, "type_code is required"):
            metadb.new(domain="test", parent_id="test")
        meta = metadb.new(
            domain="test",
            parent_id="test",
            type_code=CODES.multimedia,
        )
        self.assertEqual(meta.id, None)
        self.assertTrue(meta.path)

    def test_save_on_put(self):
        meta = new_meta()
        self.assertEqual(meta.id, None)
        self.db.put(BytesIO(b"content"), meta=meta)
        self.assertTrue(meta.id)
        saved = get_meta(id=meta.id)
        self.assertTrue(saved is not meta)
        self.assertEqual(saved.path, meta.path)

    def test_save_properties(self):
        meta = new_meta(properties={"mood": "Vangelis"})
        self.db.put(BytesIO(b"content"), meta=meta)
        self.assertEqual(get_meta(id=meta.id).properties, {"mood": "Vangelis"})

    def test_save_empty_properties(self):
        meta = new_meta()
        self.assertEqual(meta.properties, {})
        self.db.put(BytesIO(b"content"), meta=meta)
        self.assertEqual(get_meta(id=meta.id).properties, {})
        with connections[db_for_read_write(BlobMeta)].cursor() as cursor:
            cursor.execute(
                "SELECT id, properties FROM blobs_blobmeta WHERE id = %s",
                [meta.id],
            )
            self.assertEqual(cursor.fetchall(), [(meta.id, None)])

    def test_delete(self):
        meta = new_meta()
        self.db.put(BytesIO(b"content"), meta=meta)
        self.db.delete(meta.path)
        with self.assertRaises(BlobMeta.DoesNotExist):
            get_meta(id=meta.id)

    def test_delete_missing_meta(self):
        meta = new_meta()
        self.assertFalse(self.db.exists(meta.path))
        # delete should not raise
        self.db.metadb.delete(meta.path, 0)

    def test_bulk_delete(self):
        metas = []
        for name in "abc":
            meta = new_meta(parent_id="parent", name=name)
            meta.content_length = 0
            metas.append(meta)
            self.db.metadb.put(meta)
        a, b, c = metas
        self.db.metadb.bulk_delete([a, b])
        for meta in [a, b]:
            with self.assertRaises(BlobMeta.DoesNotExist):
                get_meta(id=meta.id)
        get_meta(id=c.id)  # should not have been deleted

    def test_bulk_delete_unsaved_meta_raises(self):
        meta = new_meta()
        with self.assertRaises(ValueError):
            self.db.metadb.bulk_delete([meta])
