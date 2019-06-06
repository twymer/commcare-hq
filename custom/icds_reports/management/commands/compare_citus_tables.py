from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from difflib import context_diff
from io import BytesIO

from django.core.management.base import BaseCommand
from django.db import connections

from custom.icds_reports.const import DISTRIBUTED_TABLES


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('citus_alias')
        parser.add_argument('monolith_alias')

    def handle(self, citus_alias, monolith_alias, **options):
        for table, _ in DISTRIBUTED_TABLES:
            citus_data = BytesIO()
            monolith_data = BytesIO()
            table_name = '{}'.format(table)
            with connections[citus_alias].cursor() as c:
                pk_cols = _get_table_pkey_cols(c, table_name)
                q = 'select * from "{}" order by {}'.format(table_name, ', '.join(pk_cols))
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

    return cursor.fetchone()[1]
