try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

DEFAULTS = OrderedDict()
DEFAULTS['DAGMAN'] = OrderedDict()
DEFAULTS['DAGMAN']['drain'] = 'no'
DEFAULTS['DAGMAN']['cancel'] = 'no'
DEFAULTS['DAGMAN']['limit_gstlal_ifo_jobs'] = 'no'
DEFAULTS['DAGMAN']['max_gstlal_ifo_jobs_per_computing_node'] = '4'
