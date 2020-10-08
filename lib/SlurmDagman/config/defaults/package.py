try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

DEFAULTS = OrderedDict()
DEFAULTS['SLURM'] = OrderedDict()
DEFAULTS['SLURM']['partition'] = ''
DEFAULTS['SLURM']['qos'] = ''
DEFAULTS['SLURM']['time_limit'] = '5-00:00:00'
DEFAULTS['SLURM']['scratch_dir'] = ''
