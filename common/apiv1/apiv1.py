# encoding: utf-8
from __future__ import unicode_literals
import json
import logging
import time
import os
from urlparse import urljoin
import requests
from common import settings

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
DEFAULT_TIMEOUT = 60


class RetryCountExceeded(Exception):
    """Raised when number of retries is exceeded"""


def _is_retry_status_code(status_code):
    return status_code == 404 or status_code >= 500


def _default_headers():
    # This X-Mousera-Authorization us because CloudFront says
    # that won't mess with your X-Authorization header but Alson says
    # he couldn't get that to work.

    headers = {
        'Accept': 'application/json'
    }

    # @TODO: Get this token into a sane place!
    token = getattr(settings, 'MOUSERA_API_TOKEN', None)
    if not token:
        token = os.environ.get('MOUSERA_API_TOKEN', None)
    if not token:
        logger.warning( "No api access token found" )

    if token:
        headers['X-Mousera-Authorization'] = 'Token %s' % token

    return headers


def _log_request_fail(method, full_url, body, duration, status_code=None, exception=None):
    """
    Log an unsuccessful API call.

      Args:
          method: string http method
          full_url: url called
          body: response body
          duration: duration of call
          status_code: status code received
          exception: exception received (if any)

    """
    logger.error(
        '%s %s [status:%s request:%.3fs]', method, full_url,
        status_code or 'N/A', duration, exc_info=exception is not None
    )

    logger.debug('> %s', body)


def _perform_request(method, url, params=None, timeout=None, headers=None, **kw):
    start = time.time()
    body = kw.get('body', None) or kw.get('json', None)

    try:
        response = requests.request(method, url,
                                    params=params,
                                    headers=headers,
                                    timeout=timeout or DEFAULT_TIMEOUT,
                                    **kw)
        duration = time.time() - start
    except requests.exceptions.SSLError as e:
        _log_request_fail(method, url, body, time.time() - start, exception=e)
        raise
    except requests.Timeout as e:
        _log_request_fail(method, url, body, time.time() - start, exception=e)
        raise
    except requests.ConnectionError as e:
        _log_request_fail(method, url, body, time.time() - start, exception=e)
        raise

    # raise errors based on http status codes, let the client handle those if needed
    if response.status_code >= 400:
        raw_data = response.text
        _log_request_fail(method, url, body, duration, response.status_code)
        logger.error('< %s' % raw_data)

    return response


def _smart_retry(method, url, **kw):
    retry_count = 0

    while retry_count < _MAX_RETRIES:
        try:
            response = _perform_request(method, url, **kw)
            if not _is_retry_status_code(response.status_code):
                # Only retry 5xx errors
                return response
        except (requests.Timeout, requests.ConnectionError) as e:
            logger.error("Received %s, retrying..." % e)

        retry_count += 1
        time.sleep(1)

    raise RetryCountExceeded("Number of retries exceeded.")


def request_json(path, method="GET", params=None, data_dict=None, headers=None):
    """
    Make a standard JSON request the api/v1 end-points
    Note, use below get_resource helpers for paged resources

    Args:
        path: request path, i.e. /api/v1/cage
        method: HTTP Method, defaults to GET
        params dict: Additional url parameters
        data_dict: is the dict to be serialized to JSON if any
        headers (dict): any additional headers
    Returns:
        deserialized JSON response as list or dict
    Raises:
        requests.exceptions.HTTPError on non-200 reply
    """
    hdrs = _default_headers()

    if headers:
        hdrs.update(headers)

    url = urljoin(settings.MOUSERA_API_URL, path)

    response = _smart_retry(method, url,
                           json=json.dumps(data_dict) if data_dict else None,
                           params=params,
                           headers=hdrs)
    response.raise_for_status()

    return response.json()


def get_resource_by_id(resource_name, resource_id, params=None, headers=None):
    """
    Make a get request to a speicifc resource id

    Args:
        resource_name:
        resource_id:
        params:
        headers:
    Returns:
        deserialized JSON response as list or dict
    Raises:
        requests.exceptions.HTTPError on non-200 reply
    """

    url = urljoin(settings.MOUSERA_API_URL, '%s/%s' % (resource_name, resource_id))
    hdrs = _default_headers()

    if headers:
        hdrs.update(headers)

    response = _smart_retry('GET', url, params=params, headers=hdrs)
    response.raise_for_status()
    return response.json()


def get_resources(resource_name, params=None, headers=None):
    """
    Fetch all of the resources by making successive calls to a paged resource.
    Paging is under the theory that it's better to have many short-lived
    requests than one long one that might time-out.

    Args:
        resource_name:
        params:
        headers
    Returns:
        deserialized JSON response as list
    Raises:
        requests.exceptions.HTTPError on non-200 reply
    """

    url = urljoin(settings.MOUSERA_API_URL, resource_name)
    data = []
    req_params = {'page_size': 100}
    if params:
        req_params.update(params)

    hdrs = _default_headers()

    if headers:
        hdrs.update(headers)

    while True:
        response = _smart_retry("GET", url, params=req_params, headers=hdrs)
        response.raise_for_status()
        data.extend(response.json()['results'])

        if response.json()['next']:
            url = response.json()['next']
            req_params = {}  # Params are already included in next
        else:
            break

    return data
