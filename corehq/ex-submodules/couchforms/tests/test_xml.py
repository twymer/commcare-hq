#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4 encoding=utf-8

import uuid
import os
from corehq.form_processor.interfaces.processor import FormProcessorInterface
from django.test import TestCase
from casexml.apps.case.tests.util import TEST_DOMAIN_NAME
from corehq.form_processor.test_utils import run_with_all_backends


class XMLElementTest(TestCase):

    @run_with_all_backends
    def test_various_encodings(self):
        tests = (
            ('utf-8', u'हिन्दी चट्टानों'),
            ('UTF-8', u'हिन्दी चट्टानों'),
            ('ASCII', 'hello'),
        )
        file_path = os.path.join(os.path.dirname(__file__), "data", "encoding.xml")
        with open(file_path, "rb") as f:
            xml_template = f.read()

        for encoding, value in tests:
            xml_data = xml_template.format(
                encoding=encoding,
                form_id=uuid.uuid4().hex,
                sample_value=value.encode(encoding),
            )
            xform = FormProcessorInterface().post_xform(xml_data)
            self.assertEqual(value, xform.form_data['test'])
            elem = xform.get_xml_element()
            self.assertEqual(value, elem.find('{http://commcarehq.org/couchforms-tests}test').text)
