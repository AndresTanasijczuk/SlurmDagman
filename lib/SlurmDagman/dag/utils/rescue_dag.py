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
import glob
import os
import re

from SlurmDagman import constants as C


def extract_rescue_number(dag_file):
    m = re.search('\.rescue([0-9]{3})$', dag_file)
    if m:
        return int(m.group(1))
    else:
        return 0


def is_rescue_dag_file(dag_file):
    return extract_rescue_number(dag_file) > 0


def get_dag_file_rootname(dag_file):
    if is_rescue_dag_file(dag_file):
        return dag_file[:-10]
    return dag_file


def get_highest_rescue_dag_file(dag_file):
    rescue_dag_files = get_all_rescue_dag_files(dag_file)
    if rescue_dag_files:
        return rescue_dag_files[-1]
    else:
        return dag_file


def get_all_rescue_dag_files(dag_file):
    dag_file_rootname = get_dag_file_rootname(dag_file)
    rescue_dag_files = glob.glob(dag_file_rootname + '.rescue[0-9][0-9][0-9]')
    if dag_file_rootname + '.rescue000' in rescue_dag_files:
        rescue_dag_files.remove(dag_file_rootname + '.rescue000')
    for rescue_dag_file in copy.copy(rescue_dag_files):
        rescue_number = extract_rescue_number(rescue_dag_file)
        if rescue_number > C.MAX_RESCUE_NUMBER:
            rescue_dag_files.remove(rescue_dag_file)
    return sorted(rescue_dag_files)


def build_rescue_dag_file_name(dag_file, rescue_number):
    dag_file_rootname = get_dag_file_rootname(dag_file)
    return dag_file_rootname + ('.rescue%03i' % (rescue_number))


def build_next_rescue_dag_file_name(dag_file):
    rescue_number = extract_rescue_number(dag_file)
    if rescue_number < C.MAX_RESCUE_NUMBER:
        return build_rescue_dag_file_name(dag_file, rescue_number + 1)
    else:
        return build_rescue_dag_file_name(dag_file, C.MAX_RESCUE_NUMBER)


def rename_rescue_dag_files(from_rescue_number, dag_file):
    rescue_dag_files = get_all_rescue_dag_files(dag_file)
    for rescue_dag_file in rescue_dag_files:
        rescue_number = extract_rescue_number(rescue_dag_file)
        if rescue_number >= from_rescue_number:
            os.rename(rescue_dag_file, rescue_dag_file + '.old')
