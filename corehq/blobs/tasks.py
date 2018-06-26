from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from datetime import datetime

from celery.task import periodic_task
from celery.schedules import crontab

from corehq.util.datadog.gauges import datadog_counter
from corehq.blobs.models import BlobExpiration, BlobMeta
from corehq.blobs import get_blob_db

log = logging.getLogger(__name__)


@periodic_task(run_every=crontab(minute=0, hour='0,12'))
def delete_expired_blobs():
    expired = list(BlobMeta.objects.raw(
        "SELECT * FROM get_expired_blobs(%s, 1000)",
        [_utcnow()],
    ))
    get_blob_db().bulk_delete(expired)
    log.info("deleted expired blobs: %r", [m.path for m in expired])
    bytes_deleted = sum(m.content_length for m in expired)
    datadog_counter('commcare.temp_blobs.bytes_deleted', value=bytes_deleted)

    legacy_exists, legacy_bytes = _delete_legacy_expired_blobs()
    if len(expired) == 1000 or legacy_exists:
        delete_expired_blobs.delay()

    return bytes_deleted + legacy_bytes


def _delete_legacy_expired_blobs():
    """Legacy blob expiration model

    This can be removed once all BlobExpiration rows have expired and
    been deleted.
    """
    blob_expirations = BlobExpiration.objects.filter(expires_on__lt=_utcnow(), deleted=False)

    db = get_blob_db()
    paths = []
    deleted_ids = []
    bytes_deleted = 0
    for blob_expiration in blob_expirations[:1000]:
        path = blob_expiration.bucket + "/" + blob_expiration.identifier
        paths.append(path)
        deleted_ids.append(blob_expiration.id)
        bytes_deleted += blob_expiration.length
        db.delete(path)

    log.info("deleted expired blobs: %r", paths)
    BlobExpiration.objects.filter(id__in=deleted_ids).delete()
    datadog_counter('commcare.temp_blobs.bytes_deleted', value=bytes_deleted)

    return blob_expirations.exists(), bytes_deleted


def _utcnow():
    return datetime.utcnow()
