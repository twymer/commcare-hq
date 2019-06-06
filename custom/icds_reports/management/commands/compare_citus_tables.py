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


        for name in model_names:
            model = app.models[name]
            if name in ignore_models:
                print('Ignoring table {}'.format(name))
                continue

            table_name = model._meta.db_table

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
                query = 'select * from "{}" where month = \'{}\' order by {}'.format(
                    table_name, month.isoformat(), ', '.join(sort_by)
                )
            else:
                qname = 'All data'
                query = 'select * from "{}" order by {}'.format(table_name, ', '.join(sort_by))

            _run_diff(table_name, qname, query, citus_alias, monolith_alias)


def _run_diff(table_name, qname, query, citus_alias, monolith_alias):
    print('Diff for table {} ({})'.format(table_name, qname))
    citus_data = BytesIO()
    monolith_data = BytesIO()

    with connections[citus_alias].cursor() as c:
        try:
            c.copy_expert('COPY ({}) TO STDOUT WITH CSV HEADER'.format(query), citus_data)
            citus_data.seek(0)
        except Exception as e:
            print("\tError querying citus")
            print("\t\t{}".format(e))
            return

    with connections[monolith_alias].cursor() as c:
        c.copy_expert(query, monolith_data)
        monolith_data.seek(0)

    monolith_lines = list(monolith_data.readlines())
    citus_lines = list(citus_data.readlines())
    if monolith_lines or citus_lines:
        diff = context_diff(citus_lines, monolith_lines, fromfile='citus', tofile='monolith')
        for d in diff:
            print(d)
    else:
        print('\tNo data for table {}'.format(table_name))
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
