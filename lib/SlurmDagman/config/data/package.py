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

import copy

from SlurmDagman.config.defaults.common import DEFAULTS as common_config_defaults
from SlurmDagman.config.defaults.package import DEFAULTS as package_config_defaults
from SlurmDagman.config.manager import ConfigManager
from SlurmDagman.config.utils.package import get_possible_package_config_locations


package_config = ConfigManager()
package_config.set_params(package_config_defaults)
package_config.set_params(common_config_defaults)
package_config.load_from_file(get_possible_package_config_locations())
defaults = {} 
for d in [package_config_defaults, common_config_defaults]:
    for section in d.keys():
        if section not in defaults:
            defaults[section] = {}
        for option in d[section].keys():
            defaults[section][option] = None
package_config.validate(defaults)
