from __future__ import absolute_import, unicode_literals

import uuid

import six

from casexml.apps.case.mock import CaseFactory, CaseStructure
from casexml.apps.phone.tests.test_sync_mode import BaseSyncTest

from corehq.form_processor.interfaces.dbaccessors import CaseAccessors, FormAccessors


class TestArchiveCase412(BaseSyncTest):
    def setUp(self):
        super(TestArchiveCase412, self).setUp()
        self.factory = CaseFactory(self.project.name)

        self.parent_id = six.text_type(uuid.uuid4())
        case = CaseStructure(
            case_id=self.parent_id,
            attrs={
                'case_type': 'parent',
                'owner_id': self.user_id,
                'update': {'name': 'mother', 'age': '61'},
            },
        )
        self.factory.create_or_update_case(case)

    def test_archive_form_returns_412_next_sync(self):
        response = self.device.get_restore_config().get_response()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.parent_id in "".join(response.streaming_content))

        xform_id = CaseAccessors(self.project.name).get_case_xform_ids(self.parent_id)[0]
        xform = FormAccessors(self.project.name).get_form(xform_id)
        xform.archive()

        response = self.device.get_restore_config().get_response()
        self.assertEqual(response.status_code, 412)
