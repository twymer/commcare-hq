from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import sys
from collections import Counter
from datetime import datetime

import csv342 as csv
from django.core.management import BaseCommand

from casexml.apps.phone.dbaccessors.sync_logs_by_user import get_last_synclog_for_user
from casexml.apps.phone.models import SyncLogSQL, properly_wrap_sync_log
from corehq.apps.users.dbaccessors.all_commcare_users import get_all_user_ids_by_domain
from corehq.sql_db.util import get_db_alias_for_partitioned_doc


def parse_date(s, default=None):
    if not s:
        return default

    try:
        return datetime.strptime(s, "%Y-%m-%d").date().replace(day=1)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('domain')
        parser.add_argument(
            '-s', '--last-sync-since', dest='since', type=parse_date,
            help='Only include users who have synced on or after this date.'
        )

    def handle(self, domain, since=None, **options):
        writer = csv.writer(sys.stdout)
        db_names = ['p{}'.format(i) for i in range(1, 11)]
        writer.writerow(['user_id', 'last_sync', 'total_cases'] + db_names)
        user_ids = get_all_user_ids_by_domain(domain, include_web_users=False)
        for user_id in user_ids:
            try:
                if since:
                    result = SyncLogSQL.objects.filter(user_id=user_id, date__gte=since).order_by('date').last()
                    synclog = properly_wrap_sync_log(result.doc) if result else None
                else:
                    synclog = get_last_synclog_for_user(user_id)
            except Exception as e:
                sys.stderr.write('{}\n'.format(e))
                synclog = None

            if synclog:
                case_ids = synclog.case_ids_on_phone
                counts_by_db = Counter([
                    get_db_alias_for_partitioned_doc(case_id) for case_id in case_ids
                ])
                ordered_counts = [counts_by_db.get(db, 0) for db in db_names]
                writer.writerow([user_id, synclog.date, len(case_ids)] + ordered_counts)
