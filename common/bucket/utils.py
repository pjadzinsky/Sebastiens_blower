import common.settings

def decorate_bucket_name(bucket_base_name, region_name=common.settings.AWS_DEFAULT_REGION2, env=app_env):
    return 'mousera-%s-%s-%s' % (region_name, env, bucket_base_name)
 