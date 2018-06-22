from __future__ import absolute_import
from __future__ import unicode_literals
import base64
import hashlib
import hmac
from datetime import datetime, timedelta
from functools import wraps

import iso8601
import six

from django.conf import settings
from django.http import HttpResponse

from corehq.util.soft_assert.api import soft_assert

ACCEPTABLE_DELAY_SECONDS = 30

_soft_assert = soft_assert(notify_admins=True)


def convert_to_bytestring_if_unicode(shared_key):
    return shared_key.encode('utf-8') if isinstance(shared_key, six.text_type) else shared_key


def get_hmac_digest(shared_key, data):
    hm = hmac.new(convert_to_bytestring_if_unicode(shared_key), data, hashlib.sha256)
    digest = base64.b64encode(hm.digest())
    return digest


def timestamp_valid(timestamp_string):
    try:
        timestamp = iso8601.parse_date(timestamp_string)
    except iso8601.ParseError:
        return False

    seconds_diff = (datetime.utcnow() - timestamp).total_seconds()
    return 0 < seconds_diff < ACCEPTABLE_DELAY_SECONDS


def validate_request_hmac(setting_name, ignore_if_debug=False):
    """
    Decorator to validate request sender using a shared secret
    to compare the HMAC of the request body with
    the value of the `X-MAC-DIGEST' header.

    Example request:

        timestamp = datetime.utcnow().isoformat()
        hmac_data = post_data + timestamp
        digest = base64.b64encode(hmac.new(shared_secret, hmac_data, hashlib.sha256).digest())
        requests.post(url, data=post_data, headers={'X-MAC-DIGEST': digest})

    :param setting_name: The name of the Django setting that holds the secret key
    :param ignore_if_debug: If set to True this is completely ignored if settings.DEBUG is True
    """
    def _outer(fn):
        shared_key = getattr(settings, setting_name, None)

        @wraps(fn)
        def _inner(request, *args, **kwargs):
            if ignore_if_debug and settings.DEBUG:
                return fn(request, *args, **kwargs)

            _soft_assert(shared_key, 'Missing shared auth setting: {}'.format(setting_name))
            expected_digest = request.META.get('HTTP_X_MAC_DIGEST', None)
            if not expected_digest or not shared_key:
                return HttpResponse(status=401)

            timestamp = request.META.get('HTTP_X_MAC_TS', None)
            if not timestamp_valid(timestamp):
                return HttpResponse(status=400)

            data = request.body + timestamp
            digest = get_hmac_digest(shared_key, data)

            if expected_digest != digest:
                return HttpResponse(status=401)

            return fn(request, *args, **kwargs)
        return _inner
    return _outer
