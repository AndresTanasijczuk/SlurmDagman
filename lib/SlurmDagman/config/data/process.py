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

from SlurmDagman.config.data.package import package_config
from SlurmDagman.config.defaults.common import DEFAULTS as common_config_defaults
from SlurmDagman.config.defaults.process import DEFAULTS as process_config_defaults
from SlurmDagman.config.manager import ConfigManager

process_config = ConfigManager()
process_config.set_params(common_config_defaults)
process_config.set_params(package_config.get_params(common_config_defaults))
process_config.set_params(process_config_defaults)
