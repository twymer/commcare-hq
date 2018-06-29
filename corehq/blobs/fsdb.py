"""Filesystem database for large binary data objects (blobs)
"""
from __future__ import absolute_import
from __future__ import unicode_literals
import os
from collections import namedtuple
from os.path import commonprefix, exists, isabs, isdir, dirname, join, realpath, sep

from corehq.blobs.exceptions import BadName, NotFound
from corehq.blobs.interface import AbstractBlobDB, SAFENAME
from corehq.util.datadog.gauges import datadog_counter
from io import open

CHUNK_SIZE = 4096


class FilesystemBlobDB(AbstractBlobDB):
    """Filesystem storage for large binary data objects
    """

    def __init__(self, rootdir):
        super(FilesystemBlobDB, self).__init__()
        assert isabs(rootdir), rootdir
        self.rootdir = rootdir

    def put(self, content, **blob_meta_args):
        meta = self.metadb.new(**blob_meta_args)
        fs_path = self.get_path(meta.path)
        dirpath = dirname(fs_path)
        if not isdir(dirpath):
            os.makedirs(dirpath)
        length = 0
        with open(fs_path, "wb") as fh:
            while True:
                chunk = content.read(CHUNK_SIZE)
                if not chunk:
                    break
                fh.write(chunk)
                length += len(chunk)
        meta.content_length = length
        self.metadb.put(meta)
        return meta

    def get(self, path):
        fs_path = self.get_path(path)
        if not exists(fs_path):
            datadog_counter('commcare.blobdb.notfound')
            raise NotFound(path)
        return open(fs_path, "rb")

    def size(self, path):
        fs_path = self.get_path(path)
        if not exists(fs_path):
            datadog_counter('commcare.blobdb.notfound')
            raise NotFound(path)
        return _count_size(fs_path).size

    def exists(self, path):
        return exists(self.get_path(path))

    def delete(self, path):
        fs_path = self.get_path(path)
        file_exists = exists(fs_path)
        if file_exists:
            size = _count_size(fs_path).size
            os.remove(fs_path)
        else:
            size = 0
        self.metadb.delete(path, size)
        return file_exists

    def bulk_delete(self, metas):
        success = True
        for meta in metas:
            fs_path = self.get_path(meta.path)
            if not exists(fs_path):
                success = False
            else:
                os.remove(fs_path)
        self.metadb.bulk_delete(metas)
        return success

    def copy_blob(self, content, path):
        fs_path = self.get_path(path)
        dirpath = dirname(fs_path)
        if not isdir(dirpath):
            os.makedirs(dirpath)
        with open(fs_path, "wb") as fh:
            while True:
                chunk = content.read(CHUNK_SIZE)
                if not chunk:
                    break
                fh.write(chunk)

    def get_path(self, path):
        return safejoin(self.rootdir, path)


def safejoin(root, subpath):
    """Join root to subpath ensuring that the result is actually inside root
    """
    if (subpath.startswith(("/", ".")) or
            "/../" in subpath or
            subpath.endswith("/..") or
            not SAFENAME.match(subpath)):
        raise BadName("unsafe path name: %r" % subpath)
    root = realpath(root)
    path = realpath(join(root, subpath))
    if commonprefix([root + sep, path]) != root + sep:
        raise BadName("invalid relative path: %r" % subpath)
    return path


def _count_size(path):
    if isdir(path):
        count = 0
        size = 0
        for root, dirs, files in os.walk(path):
            count += len(files)
            size += sum(os.path.getsize(join(root, name)) for name in files)
    else:
        count = 1
        size = os.path.getsize(path)
    return _CountSize(count, size)


_CountSize = namedtuple("_CountSize", "count size")
