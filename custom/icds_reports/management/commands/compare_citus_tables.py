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

    def handle(self, citus_alias, monolith_alias, month, **options):
        for table, _ in DISTRIBUTED_TABLES:
            citus_data = BytesIO()
            monolith_data = BytesIO()
            table_name = '{}'.format(table)
            with connections[citus_alias].cursor() as c:
                c.copy_to(citus_data, table_name)
                citus_data.seek(0)
            with connections[monolith_alias].cursor() as c:
                c.copy_to(monolith_data, table_name)
                monolith_data.seek(0)
            print('Diff for table {}'.format(table_name))
            diff = context_diff(citus_data.readlines(), monolith_data.readlines(), fromfile='citus', tofile='monolith')
            for d in diff:
                print(d)
            print('--------------------')
