try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

DEFAULTS = OrderedDict()
DEFAULTS['DAGMAN'] = OrderedDict()
DEFAULTS['DAGMAN']['sleep_time'] = '120'
DEFAULTS['DAGMAN']['max_jobs_queued'] = '500'
DEFAULTS['DAGMAN']['max_jobs_submit'] = '0'
DEFAULTS['DAGMAN']['submit_wait_time'] = '2'
