from __future__ import unicode_literals
from __future__ import absolute_import
import os
from io import BytesIO, open
from os.path import isdir, join
from shutil import rmtree
from tempfile import mkdtemp
from django.test import TestCase

import corehq.blobs.fsdb as mod
from corehq.blobs.tests.util import new_meta
from corehq.util.test_utils import generate_cases, patch_datadog


class _BlobDBTests(object):

    def test_put_and_get(self):
        identifier = new_meta()
        meta = self.db.put(BytesIO(b"content"), meta=identifier)
        self.assertEqual(identifier, meta)
        with self.db.get(meta.path) as fh:
            self.assertEqual(fh.read(), b"content")

    def test_put_and_size(self):
        identifier = new_meta()
        with patch_datadog() as stats:
            meta = self.db.put(BytesIO(b"content"), meta=identifier)
        size = len(b'content')
        self.assertEqual(sum(s for s in stats["commcare.blobs.added.count"]), 1)
        self.assertEqual(sum(s for s in stats["commcare.blobs.added.bytes"]), size)
        self.assertEqual(self.db.size(meta.path), size)

    def test_put_and_get_with_unicode_names(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        with self.db.get(meta.path) as fh:
            self.assertEqual(fh.read(), b"content")

    def test_put_from_get_stream(self):
        old = self.db.put(BytesIO(b"content"), meta=new_meta())
        with self.db.get(old.path) as fh:
            new = self.db.put(fh, meta=new_meta())
        with self.db.get(new.path) as fh:
            self.assertEqual(fh.read(), b"content")

    def test_exists(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        self.assertTrue(self.db.exists(meta.path), 'not found')

    def test_delete_not_exists(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        self.db.delete(meta.path)
        self.assertFalse(self.db.exists(meta.path), 'not deleted')

    def test_delete(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        self.assertTrue(self.db.delete(meta.path), 'delete failed')
        with self.assertRaises(mod.NotFound):
            self.db.get(meta.path)
        return meta

    def test_bulk_delete(self):
        metas = [
            self.db.put(BytesIO(b"content-{}".format(path)), meta=new_meta())
            for path in ['test.5', 'test.6']
        ]

        with patch_datadog() as stats:
            self.assertTrue(self.db.bulk_delete(metas), 'delete failed')
        self.assertEqual(sum(s for s in stats["commcare.blobs.deleted.count"]), 2)
        self.assertEqual(sum(s for s in stats["commcare.blobs.deleted.bytes"]), 28)

        for meta in metas:
            with self.assertRaises(mod.NotFound):
                self.db.get(meta.path)

        return metas

    def test_delete_no_args(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        with self.assertRaises(TypeError):
            self.db.delete()
        with self.db.get(meta.path) as fh:
            self.assertEqual(fh.read(), b"content")
        self.assertTrue(self.db.delete(meta.path))

    def test_empty_attachment_name(self):
        meta = self.db.put(BytesIO(b"content"), meta=new_meta())
        self.assertNotIn(".", meta.path)
        return meta

    def test_put_with_colliding_blob_id(self):
        meta = new_meta()
        self.db.put(BytesIO(b"bing"), meta=meta)
        self.db.put(BytesIO(b"bang"), meta=meta)
        with self.db.get(meta.path) as fh:
            self.assertEqual(fh.read(), b"bang")


@generate_cases([
    ("\u4500.1/test.1",),
    ("/tmp/notallowed/test.1",),
    ("./test.1",),
    ("../test.1",),
    ("../notallowed/test.1",),
    ("notallowed/../test.1",),
    ("/test.1",),
], _BlobDBTests)
def test_bad_name(self, path):
    with self.assertRaises(mod.BadName):
        self.db.get(path)


class TestFilesystemBlobDB(TestCase, _BlobDBTests):

    @classmethod
    def setUpClass(cls):
        super(TestFilesystemBlobDB, cls).setUpClass()
        cls.rootdir = mkdtemp(prefix="blobdb")
        cls.db = mod.FilesystemBlobDB(cls.rootdir)

    @classmethod
    def tearDownClass(cls):
        cls.db = None
        rmtree(cls.rootdir)
        cls.rootdir = None
        super(TestFilesystemBlobDB, cls).tearDownClass()

    def test_delete(self):
        meta = super(TestFilesystemBlobDB, self).test_delete()
        self.assertFalse(self.db.delete(meta.path), 'delete should fail')

    def test_bulk_delete(self):
        paths = super(TestFilesystemBlobDB, self).test_bulk_delete()
        self.assertFalse(self.db.bulk_delete(paths), 'delete should fail')

    def test_blob_path(self):
        meta = new_meta(path=join("doctype", "8cd98f0", "blob_id"))
        self.db.put(BytesIO(b"content"), meta=meta)
        path = os.path.dirname(self.db.get_path(meta.path))
        self.assertTrue(isdir(path), path)
        self.assertTrue(os.listdir(path))

    def test_empty_attachment_name(self):
        meta = super(TestFilesystemBlobDB, self).test_empty_attachment_name()
        path = self.db.get_path(meta.path)
        with open(path, "rb") as fh:
            self.assertEqual(fh.read(), b"content")
