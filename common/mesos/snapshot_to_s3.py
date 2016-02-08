# snapshot marathon and chronos state, and log to an s3 bucket.

import sys
import json

from common import settings

import common.mesos.marathon
import common.mesos.chronos

import boto.s3
import boto.s3.key

conn = boto.s3.connect_to_region(settings.AWS_DEFAULT_REGION)
bucket = conn.get_bucket(settings.MESOS_SNAPSHOTS_BUCKET, validate=False)

args = None

def get_snapshot():
    apps = common.mesos.marathon.get_apps() # XXX fix this to be quiet please
    jobs = common.mesos.chronos.get_jobs()
    result = { 'marathon': apps, 'chronos': jobs }

    return result


def update_snapshot():
    snapshot = get_snapshot()

    # get latest.json from s3
    # compare with latest; if same, do nothing
    # if different, write latest.json AND timestamp.json

    key = bucket.get_key('latest.json')
    if key is None:
        latest = ''
    else:
        latest = key.get_contents_as_string()
    snapshot_str = json.dumps(snapshot, indent=4, sort_keys=True)

    if snapshot_str == latest:
        return False

    from datetime import datetime
    timestamp_str = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    for name in ('latest.json', timestamp_str + '.json'):
        key = bucket.new_key(name)
        key.set_contents_from_string(snapshot_str)

    return snapshot_str


def main():
    import argparse
    parser = argparse.ArgumentParser()
    global args
    args = parser.parse_args()

    from datetime import datetime
    result = update_snapshot()
    if result:
        print result

    return 0


if __name__ == '__main__':
    sys.exit(main())
