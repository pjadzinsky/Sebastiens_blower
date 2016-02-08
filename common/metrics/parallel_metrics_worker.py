import json
import sys
from multiprocessing import Pool
from common.metrics.metric_client import MetricClient

def do_work( args ):
    mc = MetricClient()
    override_cache = args[0]

    # REMOVE the first item from the args tuple
    args = tuple( [ x for i, x in enumerate(args) if i > 0 ] )
    sys.stderr.write( "FETCHING " + str(list(args)) + '\n' )
    try:
        # The multiprocessing Pool system's response to an exception
        # from ANY worker is to kill the entire pool of jobs.
        # It is therefore  important that we don't this exception
        # to bubble as it would kill the entire parallel search.
        if override_cache:
            results = mc.get_metrics( *list(args) )
        else:
            results = mc.get_metrics_cached( *list(args) )
    except Exception, e:
        sys.stderr.write( "ERROR FETCHING " + str(list(args)) + '\n' )
        results = { 'exception': str(e) }
    sys.stderr.write( "DONE FETCHING " + str(list(args)) + '\n' )
    return {
        'source_id': args[0],
        'metric_names': args[1],
        'start_time': args[2],
        'end_time': args[3],
        'group_by_tags': args[4],
        'aggregation_seconds': args[5],
        'aggregation_names': args[6],
        'filter_by_tags': args[7],
        'return_timestamps_as_seconds': args[8],
        'results': results
    }


if __name__ == '__main__':

    with open( sys.argv[1], 'r' ) as f:
        command = json.loads( f.read() )

    queue = []
    for i in command['fetches']:
        args = (
            command['override_cache'],
            i['source_id'],
            i['metric_names'],
            i['start_time'],
            i['end_time'],
            i['group_by_tags'],
            i['aggregation_seconds'],
            i['aggregator_names'],
            i['filter_by_tags'],
            i['return_timestamps_as_seconds']
        )
        queue.append( args )

    pool_size = command.get('pool_size',20)
    sys.stderr.write( 'Using pool size ' + str(pool_size) + '\n' )
    pool = Pool( pool_size )
    ret = pool.map( do_work, queue )

    sys.stdout.write( json.dumps(ret,indent=4) )
