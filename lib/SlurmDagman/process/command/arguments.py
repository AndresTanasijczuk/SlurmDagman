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
import os
import pwd
import subprocess
import sys

from argparse import ArgumentParser

from SlurmDagman import constants as C
from SlurmDagman.config.data.package import package_config
from SlurmDagman.dag.utils.rescue_dag import get_dag_file_rootname, get_highest_rescue_dag_file, extract_rescue_number, build_rescue_dag_file_name, rename_rescue_dag_files 


def exit(msg):
    print('ERROR: %s' % (msg))
    sys.exit(1)


options = {}

parser = ArgumentParser(description="Submit a DAG to SLURM via slurm_dagman")

parser.add_argument("--logfile",
                    dest = "logfile",
                    default = None,
                    help = "(slurm_dagman log file)")

parser.add_argument("--outfile",
                    dest = "outfile",
                    default = None,
                    help = "(slurm_dagman out file)")

parser.add_argument("--do-rescue-from",
                    type = int,
                    dest = "do_rescue_from",
                    default = 0,
                    help = "(run rescue DAG of given number)")

parser.add_argument("--no-rescue",
                    action = "store_true",
                    dest = "no_rescue",
                    default = False,
                    help = "(ignore rescue DAGs)")

user = getpass.getuser()
uid = pwd.getpwnam(user).pw_uid
parser.add_argument("--use-proxy",
                    action = "store_true",
                    dest = "use_proxy",
                    default = False,
                    help = "(add x509 user proxy to environment)")

parser.add_argument("--proxy-file",
                    dest = "proxy_file",
                    default = '/tmp/x509up_u%s' % (uid),
                    help = "(path to x509 user proxy)")

parser.add_argument("--sleep-time",
                    type = int,
                    dest = "sleep_time",
                    default = int(package_config.get_param('DAGMAN', 'sleep_time')),
                    help = "(time -in seconds- to sleep between two slurm dagman iterations)")

parser.add_argument("--max-jobs-queued",
                    type = int,
                    dest = "max_jobs_queued",
                    default = int(package_config.get_param('DAGMAN', 'max_jobs_queued')),
                    help = "(maximum number of jobs that can be put in the slurm queue)")

parser.add_argument("--max-jobs-submit",
                    type = int,
                    dest = "max_jobs_submit",
                    default = int(package_config.get_param('DAGMAN', 'max_jobs_submit')),
                    help = "(maximum number of jobs that can be submitted per iteration)")

parser.add_argument("--submit-wait-time",
                    type = int,
                    dest = "submit_wait_time",
                    default = int(package_config.get_param('DAGMAN', 'submit_wait_time')),
                    help = "(time -in seconds- to wait after a job submission)")

parser.add_argument("dagfile",
                    nargs = 1,
                    help = "(a DAG file)")

args = parser.parse_args()

if args.do_rescue_from < 0:
    parser.error('--do-rescue-from must be a non-negative interger (smaller than or equal to %s)' % (C.MAX_RESCUE_NUMBER))
if args.do_rescue_from > C.MAX_RESCUE_NUMBER:
    parser.error('--do-rescue-from must be smaller than or equal to %s' % (C.MAX_RESCUE_NUMBER))
if args.do_rescue_from > 0 and args.no_rescue:
    parser.error('--no-rescue can not be specified together with a non zero value of --do-rescue-from')
options['do_rescue_from'] = args.do_rescue_from

dag_file_rootname = get_dag_file_rootname(args.dagfile[0])

if args.do_rescue_from == 0:
    if args.no_rescue:
        options['dag_file'] = dag_file_rootname
    else:
        options['dag_file'] = get_highest_rescue_dag_file(dag_file_rootname)
else:
    options['dag_file'] = build_rescue_dag_file_name(dag_file_rootname, args.do_rescue_from)
options['dag_file'] = os.path.expanduser(os.path.abspath(options['dag_file']))

if not os.path.isfile(options['dag_file']):
    if args.do_rescue_from > 0:
        parser.error('--do-rescue-from %i specified, but rescue DAG file %s does not exist!' % (args.do_rescue_from, options['dag_file']))
    else:
        exit('DAG file %s does not exist!' % (options['dag_file']))

if args.do_rescue_from > 0:
    rename_rescue_dag_files(args.do_rescue_from + 1, options['dag_file'])

options['no_rescue'] = args.no_rescue
if args.logfile is None:
    options['logfile'] = dag_file_rootname + '.slurm_dagman.log'
else:
    options['logfile'] = args.logfile
options['logfile'] = os.path.expanduser(os.path.abspath(options['logfile']))
if args.outfile is None:
    options['outfile'] = dag_file_rootname + '.slurm_dagman.out'
else:
    options['outfile'] = args.outfile
options['outfile'] = os.path.expanduser(os.path.abspath(options['outfile']))

if args.use_proxy:
    options['proxy_file'] = os.path.expanduser(os.path.abspath(args.proxy_file))
    if not os.path.isfile(options['proxy_file']):
        exit('Proxy file %s does not exist!' % (options['proxy_file']))
else:
    options['proxy_file'] = None
options['use_proxy'] = args.use_proxy

options['sleep_time'] = max(args.sleep_time, 0)
options['max_jobs_queued'] = max(args.max_jobs_queued, 0)
options['max_jobs_submit'] = max(args.max_jobs_submit, 0)
options['submit_wait_time'] = max(args.submit_wait_time, 0)
