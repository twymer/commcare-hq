import logging

from django.core.management.base import BaseCommand

from dimagi.utils.couch.database import iter_docs

from corehq.apps.app_manager.models import (
    LATEST_APK_VALUE,
    LATEST_APP_VALUE,
    GlobalAppConfig,
)
from corehq.dbaccessors.couchapps.all_docs import get_all_docs_with_doc_types, get_doc_count_by_type
from corehq.util.couchdb_management import couch_config

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """
        Adds any missing GlobalAppConfig objects, based on equivalent couch documents.
    """

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            dest='dry_run',
            default=False,
            help='Do not actually modify the database, just verbosely log what will happen',
        )

    def handle(self, dry_run=False, **options):
        log_prefix = "[DRY RUN] " if dry_run else ""

        logger.info("{}Found {} couch docs and {} sql models".format(
            log_prefix,
            get_doc_count_by_type(couch_config.get_db('apps'), 'GlobalAppConfig'),
            GlobalAppConfig.objects.count()
        ))
        for doc in get_all_docs_with_doc_types(couch_config.get_db('apps'), ['GlobalAppConfig']):
            log_message = "{}Created model for domain {} app {}".format(log_prefix, doc['domain'], doc['app_id'])
            if dry_run:
                if not GlobalAppConfig.objects.filter(domain=doc['domain'], app_id=doc['app_id']).exists():
                    logger.info(log_message)
            else:
                model, created = GlobalAppConfig.objects.get_or_create(
                    domain=doc['domain'],
                    app_id=doc['app_id'],
                    defaults={
                        "apk_prompt": doc['apk_prompt'],
                        "app_prompt": doc['app_prompt'],
                        "apk_version": doc.get('apk_version', LATEST_APK_VALUE),
                        "app_version": doc.get('app_version', LATEST_APP_VALUE),
                    })
                if created:
                    logger.info(log_message)
