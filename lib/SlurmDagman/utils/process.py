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

import getpass
import subprocess


def get_current_effective_user():
    return getpass.getuser()


def get_user_processes(user):
    cmd = ['ps', '-u', '%s' % (user), 'h', '-o', 'pid,command']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    user_processes = {}
    for l in out.splitlines():
        pid, command = l.split(' ', 1)
        user_processes[pid] = command
    return user_processes


def find_slurm_dagman_pids(processes):
    pids = []
    for pid, command in processes.items():
        if command.split(' ')[0] == 'python' and command.split(' ')[1].split('/')[-1] == 'slurm_dagman':
            pids.append(pid)
    return pids


def find_user_slurm_dagman_pids(user):
    user_processes = get_user_processes(user)
    user_slurm_dagman_pids = find_slurm_dagman_pids(user_processes)
    return user_slurm_dagman_pids


def find_slurm_dagman_pids_for_current_user():
    user = get_current_effective_user()
    user_slurm_dagman_pids = find_user_slurm_dagman_pids(user)
    return user, user_slurm_dagman_pids


def send_signal(pid, signal):
    cmd = ['kill', '-%s' % (signal), '%s' % (pid)]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    out = out.decode('utf-8')
    err = err.decode('utf-8')
    return out, err
