import subprocess
import json
import tempfile

'''
ZBS 1 Mar 2016.

  Parallelization of the metrics calls turns out to be a hard problem.

  Naive usage of the multiprocessing library locks up as there is some
threading vs. forking contention over the "requests" library whereby
if you make a request.* call before you fork then the child pid locks
up on the S3 calls. Presumably this is a threading resource that is
locked in the parent process.

  Furthermore, attempts to use the threading library alone in place of
the forking multiprocessing library also fail sporadically for unknown
reasons it will occasionally exception and even seg fault.

  The implementation here is to use mutliprocessing library but have
the metric_client launch the worker processes as a new process (not a fork) so that
it won't lock up on the requests library locks.

USAGE:

pm = ParallelMetrics()
# Make a bunch of calls to pm.get_metrics_cached( ... )
results = pm.join()
for result in results:
    # Each record contains the arguments that you requested
    # (Not necessarily in the same order you requested them)
    # And the results themselves are in result['results']
    result['source_id']
    result['metric_names']
    result['start_time']
    result['end_time']
    result['group_by_tags']
    result['aggregation_seconds']
    result['aggregation_names']
    result['filter_by_tags']
    result['return_timestamps_as_seconds']
    result['results']
'''


class ParallelMetrics(object):
    def __init__(self):
        self.fetches = []

    def get_metrics_cached(
        self, source_id, metric_names, start_time, end_time, group_by_tags=None, aggregation_seconds=None,
        aggregator_names=None, filter_by_tags=None, return_timestamps_as_seconds=False
    ):
        self.fetches.append(
            {
                "source_id": source_id,
                "metric_names": metric_names,
                "start_time": float(start_time),
                "end_time": float(end_time),
                "group_by_tags": group_by_tags,
                "aggregation_seconds": aggregation_seconds,
                "aggregator_names": aggregator_names,
                "filter_by_tags": filter_by_tags,
                "return_timestamps_as_seconds": return_timestamps_as_seconds
            }
        )

    def join(self,pool_size=16,override_cache=False):
        # This launches a subprocess to do the fetches
        # The override_cache is used for testing
        command = {
            'pool_size': pool_size,
            'override_cache': override_cache,
            'fetches': self.fetches
        }

        with tempfile.NamedTemporaryFile() as temp:
            temp.write( json.dumps(command, indent=4) )
            temp.flush()
            output = subprocess.check_output( ["python", "parallel_metrics_worker.py", temp.name] )
            return json.loads( output )