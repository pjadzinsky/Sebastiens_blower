#THIS FILE IS DEPRECATED AND WILL BE REMOVED AFTER THE V2->V3 migration. Use metric_file_naming.py and metric_name_parsing.py instead.
# Provides mappings from metric names to other objects.

# Map old metric names to new metric names.
import calendar
import datetime
from common.metrics import metric_time
import os.path
import pytz
import re

v2_to_v3_metric_name_mapping = {
    '.motion.metrics': {
        'motion.1s.mean': 'motion.1s.mean',
        'motion': 'motion',
        'video.chunk.duration': {'front': 'video.chunk.duration.front', 'back': 'video.chunk.duration.back', 'default': 'video.chunk.duration.unknown_position'},
    },


    '.weight.metrics': {
        'weight': 'weight',
        'weight.std': 'weight.std',

    },

    '.respiration.metrics': {
        'respiration': 'respiration',
    },

    '.illumination.metrics': {
        'pi_AmbientLight.lux2': 'light.lux2',
    },

    '.slab.metrics': {
        'pi_AirflowTemperature.heat': {'outlet': 'airflow.heat.out', 'inlet': 'airflow.heat.in', 'default' : 'airflow.heat'},
        'pi_AirflowTemperature.temperature': {'outlet': 'airflow.temperature.out', 'inlet': 'airflow.temperature.in', 'default' : 'airflow.temperature'},
        'pi_scale_PiScaleWorker.mass_grams': 'scale.mass',
        'pi_AirflowTemperature.heated_temperature': {'front': 'airflow.heated_temperature.out', 'back': 'airflow.heated_temperature.in', 'default' : 'airflow.heated_temperature'},
        'pi_Humidity.humidity': {'outlet': 'humidity.out', 'inlet': 'humidity.in', 'default' : 'humidity'},
        'pi_Humidity.temperature': {'outlet': 'humidity.temperature.out', 'inlet': 'humidity.temperature.in', 'default' : 'humidity.temperature'},
        'pi_Gas.nh3': {'outlet': 'gas.nh3.out', 'inlet': 'gas.nh3.in', 'default' : 'gas.nh3'},
        'pi_Gas.ox': {'outlet': 'gas.ox.out', 'inlet': 'gas.ox.in', 'default' : 'gas.ox'},
        'pi_Gas.red': {'outlet': 'gas.red.out', 'inlet': 'gas.red.in', 'default' : 'gas.red'},
        'pi_AmbientTemperature.temperature': 'ambient_temperature',
        'pi_AmbientLight.lux': 'light.lux',
        'pi_AmbientLight.visible':'light.visible',
        'pi_AmbientLight.infrared': 'light.infrared',
        'pi_IRTemperature.objtemp': 'ir.temperature',
        'pi_IRTemperature.dietemp': 'ir.die_temperature',
        'pi_scale_PiScaleWorker.battery_capacity' : 'scale.battery.capacity',
        'pi_scale_PiScaleWorker.battery_voltage' : 'scale.battery.voltage',
        'device_health_DeviceHealth.disk_free': {'front': 'device_health.disk_free.front','back': 'device_health.disk_free.back', 'default': 'device_health.disk_free'},
        'device_health_DeviceHealth.disk_used_percentage' : {'front': 'device_health.disk_used_percentage.front','back': 'device_health.disk_used_percentage.back', 'default': 'device_health.disk_used_percentage'},
        'device_health_DeviceHealth.load_avg' : {'front': 'device_health.load_avg.front','back': 'device_health.load_avg.back', 'default': 'device_health.load_avg'},
        'device_health_DeviceHealth.memory_free' : {'front': 'device_health.memory_free.front','back': 'device_health.memory_free.back', 'default': 'device_health.memory_free'},
        'device_health_DeviceHealth.memory_used_percentage' : {'front': 'device_health.memory_used_percentage.front','back': 'device_health.memory_used_percentage.back', 'default': 'device_health.memory_used_percentage'},
        'device_health_DeviceHealth.device_health.boot' : {'front': 'device_health.boot.front','back': 'device_health.boot.back', 'default': 'device_health.boot'},
        'device_health_Heartbeat.device_health.heartbeat' : {'front': 'device_health.heartbeat.front','back': 'device_health.heartbeat.back', 'default': 'device_health.heartbeat'},
        'pi_scale_PiScaleWorker.loadcell_0' : 'scale.loadcell_0',
        'pi_scale_PiScaleWorker.loadcell_1' : 'scale.loadcell_1',
        'pi_scale_PiScaleWorker.loadcell_2' : 'scale.loadcell_2',
        'pi_scale_PiScaleWorker.scale_uid' : 'scale.uid',
        'power_RackPower.current': 'rack_power.current',
        'power_RackPower.voltage': 'rack_power.voltage',
        'power_RackPower.power': 'rack_power.power',
        'pi_video_PiVideo.time_sec': {'front':'video.time_sec.front', 'back':'video.time_sec.back', 'default':'video.time_sec'},
        'pi_video_PiVideo.cage_id': {'front':'video.cage_id.front', 'back':'video.cage_id.back', 'default':'video.cage_id'},
        'pi_video_PiVideo.device_id': {'front':'video.device_id.front', 'back':'video.device_id.back', 'default':'video.device_id'},
        'pi_video_PiVideo.device_health.mmal_error': {'front':'device_health.mmal_error.front', 'back':'device_health.mmal_error.back', 'default':'device_health.mmal_error'},
        'pi_video_GpuHealthWorker.device_health.gpu_unhealthy':  {'front':'device_health.gpu_unhealthy.front', 'back':'device_health.gpu_unhealthy.back', 'default':'device_health.gpu_unhealthy'},
    },

    '.airflow.metrics': {
        'pi_AirflowTemperature.airflow': {'outlet': 'airflow.out', 'inlet':'airflow.in', 'default' : 'airflow'},
        'pi_AirflowTemperature.airflow-alert': {'outlet': 'airflow.airflow_alert.out', 'inlet': 'airflow.airflow_alert.in', 'default' : 'airflow.airflow_alert'},
        'pi_AirflowTemperature.variance': {'outlet': 'airflow.airflow_uncertainty.out', 'inlet': 'airflow.airflow_uncertainty.in', 'default' : 'airflow.airflow_uncertainty'},
    },

    '.active_period.metrics': {
        'active_period.main.duration': 'active_period.main.duration',
        'active_period.main.count': 'active_period.main.count',
    }


    # TODO: go through some example metrics files
}



def strip_aggregation(metric_name):
    aggregate_suffixes = ["\.mean$",
                    "\.max$",
                    "\.min$",
                    "\.count$",
                    "\.sd$"]

    suffixes_re = "(\.[0-9]{2,}s(" + "|".join(aggregate_suffixes) + "))"
    m = re.match("(.*)" + suffixes_re + "$", metric_name)
    if m:
        groups = m.groups()
        if len(groups)>2:
            stripped = groups[0]
            aggregate_suffix = groups[1]
            return (stripped, aggregate_suffix)
    return (metric_name, None)


def get_metric_file_suffix(v2_name):
    (v2_name, aggregate) = strip_aggregation(v2_name)
    suffix = None
    for key in v2_to_v3_metric_name_mapping.iterkeys():
        if v2_to_v3_metric_name_mapping[key].get(v2_name):
            suffix= key
    if not suffix:
        suffix = ".catchall.metrics"
    if aggregate:
        suffix = suffix.replace(".metrics", ".aggregate.metrics")
    return suffix

def build_v3_name_to_suffix_map():
    result_dict = {}
    #Builds a reverse map v3_name --> file suffix
    for current_suffix in v2_to_v3_metric_name_mapping.iterkeys():
        metrics_in_current_suffix = v2_to_v3_metric_name_mapping[current_suffix]
        for v2_name in metrics_in_current_suffix:
            v3_name_value = metrics_in_current_suffix[v2_name]
            if isinstance(v3_name_value, basestring):
                result_dict[v3_name_value] = current_suffix
            else:
                for device_characteristic in v3_name_value:
                    result_dict[v3_name_value[device_characteristic]] = current_suffix
    return result_dict

v3_name_to_file_suffix = build_v3_name_to_suffix_map()


def get_v3_metric_file_suffix(v3_name):
    #Returns a metric file suffix out of a v3 name.
    #This will be faster when we no longer need the v2 names in the map....
    (v3_name, aggregate) = strip_aggregation(v3_name)
    suffix = v3_name_to_file_suffix.get(v3_name)
    if not suffix:
        suffix = ".catchall.metrics"
    if aggregate:
        suffix = suffix.replace(".metrics", ".aggregate.metrics")
    return suffix


class TranslationException(BaseException):
    pass


def get_v3_name(v2_name, device_characteristics):
    #returns (v3_name, translated, used 'default' mapping)
    # translated is False if we couldn't find a translation entry and fell back on v2_name
    stripped_v2_name, aggregate_suffix = strip_aggregation(v2_name)
    for key in v2_to_v3_metric_name_mapping.iterkeys():
        name_map = v2_to_v3_metric_name_mapping[key].get(stripped_v2_name)
        if name_map:
            #A string value means v3 name is independent of any device characteristics
            if isinstance(name_map, basestring):
                return (append_aggregate_suffix(name_map, aggregate_suffix), True, False)
            else:
                #Try to find a specific mapping given our device characteristics
                for characteristic in device_characteristics:
                    v3_name = name_map.get(characteristic)
                    if v3_name:
                        return (append_aggregate_suffix(v3_name, aggregate_suffix), True, False)
                #try to find a default mapping if no characteristics matched
                v3_name = name_map.get('default')
                if v3_name:
                    return (append_aggregate_suffix(v3_name, aggregate_suffix), True, True)
                raise TranslationException("'%s': device characteristics did not match but no default value given" % v2_name)
    return (v2_name, False, False)


def append_aggregate_suffix(stripped_name, aggregate_suffix):
    return stripped_name + aggregate_suffix if aggregate_suffix else stripped_name


def get_metric_filename(source_id, start_time, suffix, round_down_start_time=True):
    # returns directory, filename
    #source id will be part of a path, so strip any invalid characters
    valid_path_source_id = ''.join(c for c in source_id if c in '-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
    start_time = start_time.astimezone(pytz.utc)    #start_time must be timezone-aware as filenames use UTC time
    if round_down_start_time:
        start_time = start_time.replace(minute = start_time.minute - start_time.minute%10, second=0, microsecond = 0)
    assert start_time.minute % 10 == 0 and start_time.second == 0 and start_time.microsecond == 0, "Unsupported start time: %s; only round 10 minute start times supported" % start_time
    source_id_path_component = valid_path_source_id[::-1] if valid_path_source_id else 'no_source_id'
    directory = os.path.join(source_id_path_component, str(start_time.year), "%02d" % start_time.month,
                             "%02d" % start_time.day)
    filename = "%02d.%02d%s" % (start_time.hour, start_time.minute, suffix)
    return directory, filename


def look_up_metric_filename(v3_metric_name, source_id, time):
    # returns directory, filename
    suffix = get_v3_metric_file_suffix(v3_metric_name)
    return get_metric_filename(source_id, time, suffix, True)


def get_timestamp(time):
    return metric_time.get_timestamp(time)


def get_datetime(timestamp):
    return metric_time.get_datetime(timestamp)
