from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
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

    # too big
    'aggawc',
    'aggawcdaily',
    'aggawcdailyview',
    'aggawcmonthly',

]

sort_fields = {
    'aggawc': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggccsrecord': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggchildhealth': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],
    'aggawcdaily': ['state_id', 'district_id', 'block_id', 'supervisor_id', 'awc_id'],

    'dailyattendanceview': ['month', 'doc_id'],
    'childhealthmonthlyview': ['month', 'case_id'],
    'agglsmonthly': ['month', 'supervisor_id'],
    'servicedeliverymonthly': ['month', 'awc_id'],
    'awwincentivereportmonthly': ['month', 'awc_id'],
    'aggccsrecordmonthly': ['month', 'awc_id'],
    'ccsrecordmonthlyview': ['month', 'awc_id'],
    'aggchildhealthmonthly': ['month', 'awc_id'],
    'awclocationmonths': ['month', 'awc_id'],
    'icds_disha_indicators': ['month', 'block_id'],
}

VIEWS = [
    'dailyattendanceview',
    'childhealthmonthlyview',
    'agglsmonthly',
    'servicedeliverymonthly',
    'awwincentivereportmonthly',
    'aggccsrecordmonthly',
    'ccsrecordmonthlyview',
    'aggchildhealthmonthly',
    'awclocationmonths',
    'dishaindicatorview',
]

DISTRIBUTED_MODELS = [
    'aggregateccsrecorddeliveryforms',
    'aggregatecomplementaryfeedingforms',
    'aggregateccsrecordcomplementaryfeedingforms',
    'aggregatechildhealththrforms',
    'aggregategrowthmonitoringforms',
    'aggregatechildhealthpostnatalcareforms',
    'aggregateccsrecordpostnatalcareforms',
    'aggregatebirthpreparednesforms',
    'aggregateccsrecordthrforms',
    'aggregatechildhealthdailyfeedingforms',
    'childhealthmonthly',
    'ccsrecordmonthly',
    'dailyattendance',
]


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('citus_alias')
        parser.add_argument('monolith_alias')
        parser.add_argument('--group', choices=('all', 'distributed', 'views'), default='all')
        parser.add_argument('--model', nargs='+')

    def handle(self, citus_alias, monolith_alias, **options):
        models = options['model']
        group = options['group']

        app = apps.get_app_config('icds_reports')

        if models:
            model_names = models
        elif group == 'all':
            model_names = list(app.models)
        elif group == 'distributed':
            model_names = DISTRIBUTED_MODELS
        elif group == 'views':
            model_names = VIEWS

        print('table_name,citus_row_count,monolith_row_count,num_diffs')
        for name in model_names:
            model = app.models[name]
            if name in ignore_models:
                print('Ignoring table {}'.format(name))
                continue

            table_name = model._meta.db_table
            columns = []
            for f in model._meta.get_fields():
                try:
                    f_name = f.db_column or f.name
                except AttributeError:
                    continue
                if f_name != 'id':
                    columns.append(f_name)

            if name in sort_fields:
                sort_by = sort_fields[name]
            elif table_name in sort_fields:
                sort_by = sort_fields[table_name]
            else:
                with connections[citus_alias].cursor() as c:
                    sort_by = _get_table_pkey_cols(c, table_name)
            if not sort_by:
                print('Missing sort fields: {} ({})'.format(name, table_name))
                continue

            if table_name in VIEWS or name in VIEWS:
                month = datetime.utcnow().date().replace(day=1)
                qname = 'Data for month: {}'.format(month.isoformat())
                query = 'select {} from "{}" where month = \'{}\' order by {}'.format(
                    ', '.join(columns), table_name, month.isoformat(), ', '.join(sort_by)
                )
            else:
                qname = 'All data'
                query = 'select {} from "{}" order by {}'.format(
                    ', '.join(columns), table_name, ', '.join(sort_by)
                )

            _run_diff(table_name, qname, query, citus_alias, monolith_alias)


def _run_diff(table_name, qname, query, citus_alias, monolith_alias):
    citus_data = BytesIO()
    monolith_data = BytesIO()

    copy_query = 'COPY ({}) TO STDOUT WITH CSV HEADER'.format(query)
    with connections[citus_alias].cursor() as c:
        try:
            c.copy_expert(copy_query, citus_data)
            citus_data.seek(0)
        except Exception as e:
            print(e)
            return

    with connections[monolith_alias].cursor() as c:
        c.copy_expert(copy_query, monolith_data)
        monolith_data.seek(0)

    monolith_lines = list(monolith_data.readlines())
    citus_lines = list(citus_data.readlines())

    with open('table_rows_{}_monolith.txt'.format(table_name), 'w') as out_monolith:
        monolith_data.seek(0)
        out_monolith.write(monolith_data.read())

    with open('table_rows_{}_citus.txt'.format(table_name), 'w') as out_citus:
        citus_data.seek(0)
        out_citus.write(citus_data.read())

    if monolith_lines or citus_lines:
        with open('table_diff_{}_diff.txt'.format(table_name), 'w') as output:
            output.write('Diff for table {} ({})\n'.format(table_name, qname))
            diff = list(context_diff(citus_lines, monolith_lines, fromfile='citus', tofile='monolith'))
            difflen = len(diff)
            for d in diff:
                output.write(d)
    else:
        difflen = 0

    print('{},{},{},{}'.format(table_name, len(citus_lines), len(monolith_lines), difflen))


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
