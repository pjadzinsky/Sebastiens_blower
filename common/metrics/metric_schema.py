# @TODO: Finish getting bounds on all of these metrics for the Inf ones in particular

metric_schema = {
    'known_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
        # This is a list of device models that have been considered in the
        # following metrics_by_name list. Device models not listed here need to
        # be added into the schema in the available_in_device_models fields
        # If it isn't listed here and found during the data_exists_checker
        # scans that's considered an error.
        #
        # 22, 23, 24, 29 should have front and back
        # slabmoncam, power controllers
        # 

    'metrics_by_name': {
        'airflow.airflow_alert.in': {
            'bounds': [ -1.0, 1.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'airflow.airflow_alert.out': {
            'bounds': [ -1.0, 1.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'airflow.heat.in': {
            'bounds': [ 0.0, 10.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'airflow.heat.out': {
            'bounds': [ 0.0, 10.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'airflow.heated_temperature.in': {
            'bounds': [ 15.0, 40.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'airflow.heated_temperature.out': {
            'bounds': [ 15.0, 40.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'ambient_temperature': {
            'bounds': [ 15.0, 40.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'device_health.heartbeat.back': {
            'bounds': [ 0.0, 1.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
                # Heartbeats only occur every 29 minutes so no 10 minute
                # segment is guaranteed to have one.
        },
        'device_health.heartbeat.front': {
            'bounds': [ 0.0, 1.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
                # See above
        },
        'gas.nh3.in': {
            'bounds': [ 1e5, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'gas.nh3.out': {
            'bounds': [ 1e5, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'gas.ox.in': {
            'bounds': [ 1e4, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'gas.ox.out': {
            'bounds': [ 1e4, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'gas.red.in': {
            'bounds': [ 1e6, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'gas.red.out': {
            'bounds': [ 1e6, 1e8 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 22 ],
            'always_present': True,
        },
        'humidity.in': {
            'bounds': [ 0.0, 100.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'humidity.out': {
            'bounds': [ 0.0, 100.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'humidity.temperature.in': {
            'bounds': [ 15.0, 40.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'ir.die_temperature': {
            'bounds': [ 15.0, 45.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'ir.temperature': {
            'bounds': [ 15.0, 40.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'light.infrared': {
            'bounds': [ 50.0, 1100.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'light.lux': {
            'bounds': [ 0.05, 20.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'light.lux2': {
            'bounds': [ 0.05, 200.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'light.visible': {
            'bounds': [ 0.05, 1500.0 ],
            'upstream_requirements': [ '.slab.metrics' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'motion': {
            'bounds': [ 0.0, 1.5 ],
            'upstream_requirements': [ '.video.back.mp4', '.video.front.mp4' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': True,
        },
        'motion.zone.running_wheel.activity': {
            'bounds': [ 0.0, 0.3 ],
            'upstream_requirements': [ '.video.back.mp4', '.video.front.mp4' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
        },
        'motion.zone.running_wheel.detection': {
            'bounds': [ 0.0, 1.0 ],
            'upstream_requirements': [ '.video.back.mp4', '.video.front.mp4' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
        },
        'motion.zone.scale.activity': {
            'bounds': [ 0.0, 0.2 ],
            'upstream_requirements': [ '.video.back.mp4', '.video.front.mp4' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
        },
        'motion.zone.scale.detection': {
            'bounds': [ 0.0, 1.0 ],
            'upstream_requirements': [ '.video.back.mp4', '.video.front.mp4' ],
            'available_in_device_models': [ 10, 20, 21, 22, 23, 24, 29 ],
            'always_present': False,
        },
    }
}
