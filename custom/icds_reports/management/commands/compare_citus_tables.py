from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from difflib import context_diff
from io import BytesIO

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import connections

ignore_models = [
    'icdsmonths',
    'icdsfile',
    'aggregatesqlprofile',
    'ucrtablenamemapping',
    'icdsauditentryrecord',
    'citusdashboardexception',
    'citusdashboarddiff',
    'citusdashboardtiming',

    'aggawc',  # too big
    'aggawcdaily',  # too big

]

sort_fields = {
    'aggawc': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggccsrecord': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggchildhealth': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggawcdaily': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
}

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('citus_alias')
        parser.add_argument('monolith_alias')

    def handle(self, citus_alias, monolith_alias, **options):
        app = apps.get_app_config('icds_reports')
        for name, model in app.models.items():
            if name in ignore_models:
                print('Ignoring table {}'.format(name))
                continue

            citus_data = BytesIO()
            monolith_data = BytesIO()

            table_name = model._meta.db_table
            with connections[citus_alias].cursor() as c:
                c.execute('select 1 from pg_views where viewname = %s', [table_name])
                if c.fetchone():
                    print("Skipping view: {}".format(table_name))
                    continue

                if name in sort_fields:
                    sort_by = sort_fields[name]
                else:
                    sort_by = _get_table_pkey_cols(c, table_name)
                q = 'select * from "{}" order by {}'.format(table_name, ', '.join(sort_by))

                c.copy_expert(q, citus_data)
                citus_data.seek(0)

            with connections[monolith_alias].cursor() as c:
                c.copy_expert(q, monolith_data)
                monolith_data.seek(0)

            print('Diff for table {}'.format(table_name))
            diff = context_diff(citus_data.readlines(), monolith_data.readlines(), fromfile='citus', tofile='monolith')
            for d in diff:
                print(d)
            print('--------------------')


def _get_table_pkey_cols(cursor, table_name):
    cursor.execute("""SELECT tbl.relname AS "table", array_agg(ind_column.attname) AS columns_of_pk
    FROM pg_class tbl
      INNER JOIN pg_namespace sch ON sch.oid = tbl.relnamespace
      INNER JOIN pg_index ind ON ind.indrelid = tbl.oid
      INNER JOIN pg_class ind_table ON ind_table.oid = ind.indexrelid
      INNER JOIN pg_attribute ind_column ON ind_column.attrelid = ind_table.oid
    WHERE sch.nspname <> 'pg_toast'
      AND ind.indisprimary AND tbl.relname = %s
    GROUP BY "table";
    """, [table_name])

    row = cursor.fetchone()
    return row[1] if row else None
