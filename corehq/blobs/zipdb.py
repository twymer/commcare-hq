from __future__ import absolute_import
from __future__ import unicode_literals
import zipfile

from corehq.blobs.interface import AbstractBlobDB


class ZipBlobDB(AbstractBlobDB):
    """Blobs stored in zip file. Used for exporting a domain's blobs
    """

    def __init__(self, slug, domain):
        super(ZipBlobDB, self).__init__()
        self.zipname = get_export_filename(slug, domain)
        self._zipfile = None

    def put(self, content, **blob_meta_args):
        raise NotImplementedError

    def get(self, path):
        raise NotImplementedError

    def delete(self, path):
        raise NotImplementedError

    def bulk_delete(self, metas):
        raise NotImplementedError

    def copy_blob(self, content, path):
        # NOTE this does not save all metadata, and therefore
        # the zip file cannot be used to fully rebuild the
        # blob db state in another environment.
        self.zipfile.writestr(path, content.read())

    def exists(self, path):
        return path in self.zipfile.namelist()

    def size(self, path):
        raise NotImplementedError

    @property
    def zipfile(self):
        if self._zipfile is None:
            self._zipfile = zipfile.ZipFile(self.zipname, 'w', allowZip64=True)
        return self._zipfile

    def close(self):
        if self._zipfile:
            self._zipfile.close()


def get_export_filename(slug, domain):
    return 'export-{domain}-{slug}-blobs.zip'.format(domain=domain, slug=slug)
