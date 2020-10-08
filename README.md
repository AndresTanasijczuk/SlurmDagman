# SlurmDagman

This application allows to run (the jobs in) a DAG with [Slurm](https://slurm.schedmd.com/documentation.html) workload manager.

## Installation and configuration

SlurmDagman can be installed via `pip` ([SlurmDagman in PyPI](https://pypi.org/project/SlurmDagman))
or it can simply be downloaded from [github](https://github.com/AndresTanasijczuk/SlurmDagman).

A configuration file for SlurmDagman named _SlurmDagman.conf_
is included in the package. In the git repository it can be found in the
__etc__ folder. In the distribution it can be found in a sub-directory named
__etc/SlurmDagman__. The configuration file provides default values for Slurm
job submission options and for some SlurmDagman worker parameters.

SlurmDagman will try to read the _SlurmDagman.conf_ configuration file from the
following locations, in this order:

- `/etc/` and `/etc/SlurmDagman/`
- `$xdg_config_dir/` and  `$xdg_config_dir/SlurmDagman/` where $xdg_config_dir
  is each of the paths defined by the environment variable $XDG_CONFIG_DIRS;
  if $XDG_CONFIG_DIRS is not defined, $xdg_config_dir defaults to /etc/xdg/
- `site.USER_BASE/etc/` and `site.USER_BASE/etc/SlurmDagman/` where site.USER_BASE
  is the python variable that defines the location for a user installation
- `$xdg_config_home/` and `$xdg_config_home/SlurmDagman/` where $xdg_config_home
  is the environment variable $XDG_CONFIG_HOME when defined and not empty;
  otherwise it defaults to $HOME/.config
- `$VIRTUAL_ENV/etc/` and `$VIRTUAL_ENV/etc/SlurmDagman/` when $VIRTUAL_ENV
  is defined and not empty 

## Usage

See the [project webpage](https://andrestanasijczuk.github.io/SlurmDagman/).

## License

SlurmDagman is provided "as is" and with no warranty. This software is
distributed under the GNU General Public License; please see the LICENSE file
for details.
