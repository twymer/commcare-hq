import copy
from datetime import datetime, timedelta

from django.core.management.base import BaseCommand

from corehq.elastic import get_es_new
from corehq.util.dates import iso_string_to_datetime
from elasticsearch.helpers import reindex


class Command(BaseCommand):
    help = ("Adhoc command for ICDS xforms reindex using scan and bulk insert API")

    def add_arguments(self, parser):
        parser.add_argument('index_name')
        parser.add_argument('start_date', help='in yyyy-MM-dd format')
        parser.add_argument('end_date', help='in yyyy-MM-dd format')
        parser.add_argument('--scroll_timeout', default='100m',
            help='Elasticsearch scroll timeout such as 5m, 10m or 100m etc')
        parser.add_argument('--chunk_size', default=100, type=int)
        parser.add_argument(
            '--print_counts',
            action='store_true',
            default=False,
            help="Simply print doc counts and exit"
        )

    def handle(self, index_name, start_date, end_date, **options):
        es = get_es_new()
        scroll_timeout = options.get('scroll_timeout')
        chunk_size = options.get('chunk_size')
        print_counts = options.get('print_counts')
        old_docs_count = self._get_doc_count(es, 'xforms', start_date, end_date)
        if print_counts:
            print("Number of docs in old index",
                old_docs_count)
            print("Number of docs in new index",
                self._get_doc_count(es, index_name, start_date, end_date))
            return

        print("Total number of docs in this date range in old index",
            old_docs_count)
        query = self._base_query(start_date, end_date)
        query.update({
            "_source": {"exclude": ["_id"]}
        })
        old_index = "xforms"  # alias
        print("Number of remaining docs to be reindexed to new index ",
            self._get_doc_count(es, 'xforms', start_date, end_date))
        print("Starting reindex ", datetime.now())
        reindex(es, old_index, index_name, query=query, chunk_size=chunk_size, scroll=scroll_timeout)
        print("Reindex finished ", datetime.now())

    def _base_query(self, start_date, end_date):
        return copy.deepcopy({
            "query": {
                "range": {
                    "received_on": {
                        "gte": start_date,
                        "lt": end_date,
                        "format": "yyyy-MM-dd"
                    }
                }
            },
        })

    def _get_doc_count(self, es, index, start_date, end_date):
        query = self._base_query(start_date, end_date)
        ret = es.count(index, body=query)
        return ret['count']
