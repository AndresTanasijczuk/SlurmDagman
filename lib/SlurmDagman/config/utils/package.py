"""
Copyright (C) 2020  Universite catholique de Louvain, Belgium.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import site


def get_possible_package_config_locations():
    xdgConfigDirsDef = os.path.join(os.path.sep, 'etc', 'xdg')
    xdgConfigDirs = os.getenv('XDG_CONFIG_DIRS', xdgConfigDirsDef)
    if xdgConfigDirs.strip() == '':
        xdgConfigDirs = xdgConfigDirsDef
    xdgConfigHomeDef = os.path.join(os.path.expanduser('~'), '.config')
    xdgConfigHome = os.getenv('XDG_CONFIG_HOME', xdgConfigHomeDef)
    if xdgConfigHome.strip() == '':
        xdgConfigHome = xdgConfigHomeDef
    siteUserBase = site.USER_BASE
    virtualenv = os.getenv('VIRTUAL_ENV')
    configLocations = {
        'system'     : [os.path.join(os.path.sep, 'etc', 'SlurmDagman.conf'), os.path.join(os.path.sep, 'etc', 'SlurmDagman', 'SlurmDagman.conf')] + \
                       [os.path.join(xdgConfigDir.strip(), 'SlurmDagman.conf') for xdgConfigDir in xdgConfigDirs.split(':') if xdgConfigDir.strip()] + \
                       [os.path.join(xdgConfigDir.strip(), 'SlurmDagman', 'SlurmDagman.conf') for xdgConfigDir in xdgConfigDirs.split(':') if xdgConfigDir.strip()],
        'local'      : [os.path.join(siteUserBase, 'etc', 'SlurmDagman.conf'), os.path.join(siteUserBase, 'etc', 'SlurmDagman', 'SlurmDagman.conf')],
        'user'       : [os.path.join(xdgConfigHome, 'SlurmDagman.conf'), os.path.join(xdgConfigHome, 'SlurmDagman', 'SlurmDagman.conf')],
        'virtualenv' : [os.path.join(virtualenv, 'etc', 'SlurmDagman.conf'), os.path.join(virtualenv, 'etc', 'SlurmDagman', 'SlurmDagman.conf')] if virtualenv else [],
    }
    return configLocations['system'] + configLocations['local'] + configLocations['user'] + configLocations['virtualenv']

