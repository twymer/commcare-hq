from __future__ import absolute_import
from __future__ import unicode_literals
from datetime import datetime
from uuid import uuid4

from django.db.models import (
    BooleanField,
    CharField,
    DateTimeField,
    IntegerField,
    Model,
    PositiveIntegerField,
    PositiveSmallIntegerField,
)
from partial_index import PartialIndex

from corehq.sql_db.models import PartitionedModel, RestrictedManager


def uuid4_hex():
    return uuid4().hex


class BlobMeta(PartitionedModel, Model):
    """Metadata about an object stored in the blob db"""

    partition_attr = "parent_id"
    objects = RestrictedManager()

    domain = CharField(db_index=True, max_length=255)
    parent_id = CharField(
        max_length=255,
        db_index=True,
        help_text="Parent primary key or unique identifier",
    )
    name = CharField(
        max_length=255,
        null=True,
        blank=False,
        default=None,
        help_text="""Optional blob name.

        This field is intended to be used by doc types having multiple
        blobs associated with a single document.
        """,
    )
    path = CharField(
        unique=True,
        max_length=255,
        default=uuid4_hex,
        help_text="""Blob path in the external blob store.

        This must be a globally unique value. Historically this was
        `blob_bucket + '/' + identifier` for blobs associated with a
        couch document. Could be a UUID or the result of
        `util.random_url_id(16)`. Defaults to `uuid4().hex`.
        """,
    )
    type_code = PositiveSmallIntegerField(
        help_text="Blob type code. See `corehq.blobs.CODES`.",
    )
    content_length = PositiveIntegerField()
    content_type = CharField(max_length=255, null=True)
    created_on = DateTimeField(default=datetime.utcnow)
    expires_on = DateTimeField(default=None, null=True)

    class Meta:
        indexes = [
            PartialIndex(
                fields=['expires_on'],
                unique=False,
                where='expires_on IS NOT NULL',
            ),
        ]

    def __repr__(self):
        return "<BlobMeta id={self.id} path={self.path}>".format(self=self)


class BlobMigrationState(Model):
    slug = CharField(max_length=20, unique=True)
    timestamp = DateTimeField(auto_now=True)


class BlobExpiration(Model):
    '''
    This models records when temporary blobs should be deleted
    '''
    bucket = CharField(max_length=255)
    identifier = CharField(max_length=255, db_index=True)
    expires_on = DateTimeField(db_index=True)
    created_on = DateTimeField(auto_now=True)
    length = IntegerField()

    # This is set to True when the blob associated has been successfully deleted
    deleted = BooleanField(default=False, db_index=True)
