import logging

LOGGER = logging

#######################
# MESOS
#######################
# MARATHON_API="http://10.201.20.48:8080/v2"
MARATHON_API="http://10.201.21.9:8080/v2"


#######################
# KAIROS
#######################
KAIROS_HOST          = "X.kairosdb.mousera-private-prod.com"
KAIROS_INTERNAL_HOST = "X.kairosdb-elb-internal-prod-1670839758.us-west-2.elb.amazonaws.com" #NOTE: the dns is broken... this ELB is the only one know to work right now
KAIROS_EXTERNAL_HOST = KAIROS_INTERNAL_HOST 


#Oregon:
KAIROS2_INTERNAL_HOST = "kairosdb2-internal-nonprod.mousera.com"
KAIROS2_EXTERNAL_HOST = "kairosdb2-external-nonprod.mousera.com"



#######################
# AWS SETTINGS
#######################
AWS_DEFAULT_REGION = 'us-west-1'
SLAB_DATA_BUCKET = 'mousera-slab-data'
MESOS_SNAPSHOTS_BUCKET = 'mousera-mesos-snapshots'

HIPCHAT_MOUSEBOT_TOKEN = '2f70c5a0c1c0b45808e7d6d32617d6' # Type: Admin

#Oregon:
AWS_DEFAULT_REGION2 = 'us-west-2'
SLAB_DATA_BUCKET2_BASE_NAME = 'slab-data'   #decorated into SLAB_DATA_BUCKET2 when passed to common.bucket.bucket.Bucket()
SLAB_DATA_BUCKET2 = ('mousera-%s-%s-%s' % (AWS_DEFAULT_REGION2, app_env, SLAB_DATA_BUCKET2_BASE_NAME))
METRICS_BUCKET_BASE_NAME = "metrics"


#######################
# QUEUE NAMES
#######################
METRIC_AGGREGATION_QUEUE_NAME='mousera-metric-aggregation'
