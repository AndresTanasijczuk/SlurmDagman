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

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


class ConfigurationError(configparser.Error):

    def __init__(self, msg, section, option=None):
        configparser.Error.__init__(self, msg)
        self.section = section
        self.option = option


class ConfigManager(object):

    def __init__(self):
        super(ConfigManager, self).__init__()    
        self.config = configparser.ConfigParser()


    def set_params(self, params):
        for section in params.keys():
            for option, value in params[section].items():
                self.set_param(section, option, value)


    def set_param(self, section, option, value):
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, option, str(value))


    def get_params(self, sections_and_options=None):
        params = OrderedDict()
        if sections_and_options is None:
            for section in self.config.sections():
                if section not in params:
                    params[section] = OrderedDict()
                for option in self.config.options(section):
                    params[section][option] = self.get_param(section, option)
        else:
            for section in sections_and_options.keys():
                if section not in params:
                    params[section] = OrderedDict()
                # options could be a list or a dict (in which case we only care about the keys)
                for option in sections_and_options[section]:
                    params[section][option] = self.get_param(section, option)
        return params


    def get_param(self, section, option, cast=None):
        if cast == 'int':
            return self.config.getint(section, option)
        if cast == 'float':
            return self.config.getfloat(section, option)
        if cast == 'boolean':
            return self.config.getboolean(section, option)
        return self.config.get(section, option)


    def load_from_file(self, possible_file_locations):
        self.config.read(possible_file_locations)


    def write_to_file(self, filename):
        with open(filename, 'w') as fd:
            self.config.write(fd)


    def validate(self, sections_and_options, must_exist=False):
        for section in self.config.sections():
            if section not in sections_and_options.keys():
                msg = 'Bad configuration: invalid section name %s' % (section)
                raise ConfigurationError(msg, section)
            for option in self.config.options(section):
                if option not in sections_and_options[section]:
                    msg = 'Bad configuration: invalid option name %s in section %s' % (option, section)
                    raise ConfigurationError(msg, section, option)
        if must_exist:
            for section in sections_and_options.keys():
                if not self.config.has_section(section):
                    msg = 'Bad configuration: missing section %s' % (section)
                    raise ConfigurationError(msg, section)
                for option in sections_and_options[section]:
                    if not self.config.has_option(section, option):
                        msg = 'Bad configuration: missing option %s in section %s' % (option, section)
                        raise ConfigurationError(msg, section, option)
