
def get_name(worker, metric):
    if type(worker) in (str,unicode):
        return "%s.%s" % (worker.replace(".","_"), metric)
    else:
        wc = worker.__class__
        return "%s_%s.%s" % (wc.__module__.split('.')[-1], wc.__name__, metric)

def parse_name(db_name):
    worker_module, metric = db_name.split('.', 3)
    return worker_module.replace("_","."), metric
