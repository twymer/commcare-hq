# -*- coding: utf-8 -*-
# Generated by Django 1.11.22 on 2019-11-22 18:53

import sys
import traceback

from django.core.management import call_command
from django.db import migrations

from corehq.apps.app_manager.models import GlobalAppConfig
from corehq.dbaccessors.couchapps.all_docs import get_doc_count_by_type
from corehq.util.couchdb_management import couch_config
from corehq.util.django_migrations import skip_on_fresh_install


AUTO_MIGRATE_ITEMS_LIMIT = 1000
AUTO_MIGRATE_FAILED_MESSAGE = """
    A migration must be performed before this environment can be upgraded to the latest version of CommCareHQ.
    This migration is run using the management command populate_sql_global_app_config.
"""


def count_items_to_be_migrated():
    couch_count = get_doc_count_by_type(couch_config.get_db('apps'), 'GlobalAppConfig')
    sql_count = GlobalAppConfig.objects.count()
    return couch_count - sql_count


@skip_on_fresh_install
def _verify_sql_global_app_config(apps, schema_editor):
    to_migrate = count_items_to_be_migrated()
    migrated = to_migrate == 0
    if migrated:
        return

    if to_migrate < AUTO_MIGRATE_ITEMS_LIMIT:
        try:
            call_command('populate_sql_global_app_config')
            migrated = count_items_to_be_migrated() == 0
            if not migrated:
                print("Automatic migration failed")
        except Exception:
            traceback.print_exc()
    else:
        print("Found {} items that need to be migrated.".format(to_migrate))
        print("Too many to migrate automatically.")

    if not migrated:
        print("")
        print(AUTO_MIGRATE_FAILED_MESSAGE)
        sys.exit(1)


class Migration(migrations.Migration):

    dependencies = [
        ('app_manager', '0009_add_sqlglobalappconfig'),
    ]

    operations = [
        migrations.RunPython(_verify_sql_global_app_config,
                             reverse_code=migrations.RunPython.noop,
                             elidable=True),
    ]
