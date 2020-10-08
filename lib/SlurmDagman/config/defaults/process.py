try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

DEFAULTS = OrderedDict()
DEFAULTS['DAGMAN'] = OrderedDict()
DEFAULTS['DAGMAN']['drain'] = 'no'
DEFAULTS['DAGMAN']['cancel'] = 'no'
