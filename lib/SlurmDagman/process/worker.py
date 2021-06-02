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
import datetime
import logging
import os
import re
import shutil
import subprocess
import time

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from SlurmDagman.config.data.process import process_config
from SlurmDagman.config.manager import ConfigManager, ConfigurationError
from SlurmDagman.config.utils.converters import text_to_bool
from SlurmDagman.dag import Dag
from SlurmDagman.dag.utils.rescue_dag import build_next_rescue_dag_file_name, get_dag_file_rootname


class Worker(object):

    def __init__(self, dag_file=None, outfile=None, proxy=None,
                 sleep_time=None, max_jobs_queued=None, max_jobs_submit=None, submit_wait_time=None):
        super(Worker, self).__init__()
        self.outfile = outfile
        self.__set_logging()
        self.__init_dag(dag_file)
        self.proxy = proxy
        self.__init_params()
        self.__init_process_config()
        self.process_config.set_params(process_config.get_params())
        self.process_config_file = get_dag_file_rootname(dag_file) + '.slurm_dagman.cfg'
        self.set_params(sleep_time, max_jobs_queued, max_jobs_submit, submit_wait_time)
        self.dag_done = {}
        self.num_nodes_total = 0
        self.num_nodes_done = 0
        self.num_nodes_queued = 0
        self.num_nodes_ready = 0
        self.num_nodes_unready = 0
        self.num_nodes_failed = 0
        self.start_time = datetime.datetime.now().isoformat().split('.')[0]
        self.wckey = 'slurm_dagman_%s' % (self.start_time.lower())
        self.queued_job_ids = []


    def __init_dag(self, dag_file=None):
        self.dag = Dag()
        if dag_file:
            self.set_dag_file(dag_file)


    def reset_dag(self):
        self.__init_dag()


    def set_dag_file(self, dag_file):
        self.dag.set_dag_file(dag_file)


    def __set_logging(self):
        if self.outfile is not None:
            logging.basicConfig(format="%(asctime)-15s %(message)s", datefmt='%m/%d/%y %H:%M:%S', filename=self.outfile, level=logging.DEBUG)


    def __init_params(self):
        self.sleep_time, self.max_jobs_queued, self.max_jobs_submit, self.submit_wait_time, \
        self.drain, self.cancel \
            = self.__get_config_params(process_config).values()


    def __init_process_config(self):
        self.process_config = ConfigManager()


    def __reset_process_config(self):
        self.__init_process_config()


    def __get_process_config_params(self, fallback=False, sanitize=True):
        return self.__get_config_params(self.process_config, fallback, sanitize)

 
    def __get_config_params(self, config, fallback=False, sanitize=True):
        params = OrderedDict()
        params['sleep_time'] = self.__get_config_param(config, 'DAGMAN', 'sleep_time', fallback, 'int')
        params['max_jobs_queued'] = self.__get_config_param(config, 'DAGMAN', 'max_jobs_queued', fallback, 'int')
        params['max_jobs_submit'] = self.__get_config_param(config, 'DAGMAN', 'max_jobs_submit', fallback, 'int')
        params['submit_wait_time'] = self.__get_config_param(config, 'DAGMAN', 'submit_wait_time', fallback, 'int')
        params['drain'] = self.__get_config_param(config, 'DAGMAN', 'drain', fallback, 'boolean')
        params['cancel'] = self.__get_config_param(config, 'DAGMAN', 'cancel', fallback, 'boolean')
        if sanitize:
            for name, value in params.items():
                params[name] = self.__replace_negative_int_by_zero(value)
        return params


    def __get_config_param(self, config, section, option, fallback=False, cast=None):
        try:
            param = config.get_param(section, option, cast)
        except Exception:
            if fallback:
                param = process_config.get_param(section, option, cast)
            else:
                param = None
        return param


    def __replace_negative_int_by_zero(self, param):
        if isinstance(param, int):
            return max(param, 0)
        return param
            

    def reset_params(self):
        self.__init_params()


    def set_params(self, sleep_time=None, max_jobs_queued=None, max_jobs_submit=None, submit_wait_time=None,
                         drain=None, cancel=None):
        if sleep_time is not None:
            self.sleep_time = self.__replace_negative_int_by_zero(sleep_time)
        if max_jobs_queued is not None:
            self.max_jobs_queued = self.__replace_negative_int_by_zero(max_jobs_queued)
        if max_jobs_submit is not None:
            self.max_jobs_submit = self.__replace_negative_int_by_zero(max_jobs_submit)
        if submit_wait_time is not None:
            self.submit_wait_time = self.__replace_negative_int_by_zero(submit_wait_time)
        if drain is not None:
            self.drain = drain
        if cancel is not None:
            self.cancel = cancel
        self.__set_process_config_params()
        self.__write_process_config()


    def __set_process_config_params(self):
        self.process_config.set_param('DAGMAN', 'sleep_time', self.sleep_time)
        self.process_config.set_param('DAGMAN', 'max_jobs_queued', self.max_jobs_queued)
        self.process_config.set_param('DAGMAN', 'max_jobs_submit', self.max_jobs_submit )
        self.process_config.set_param('DAGMAN', 'submit_wait_time', self.submit_wait_time )
        self.process_config.set_param('DAGMAN', 'drain', self.drain)
        self.process_config.set_param('DAGMAN', 'cancel', self.cancel)
 

    def __write_process_config(self):
        self.process_config.write_to_file(self.process_config_file)


    def __load_process_config(self):
        # Loading a config from file into memory is an update-like operation.
        # It can only overwrite existing config parameters in memory or define
        # new ones; it can not delete existing parameters in memory.
        if os.path.isfile(self.process_config_file):
            self.process_config.load_from_file([self.process_config_file])
            return True
        return False


    def __validate_process_config(self):
        try:
            self.process_config.validate(process_config.get_params(), True)
        except ConfigurationError:
            return False
        else:
            return True


    def __parse_process_config(self, log_changes=True):
        # Put the values of the local current process config in memory into
        # class variables. We don't want to fallback to use the global initial
        # process config if a parameter fails to be read from the local current
        # process config; instead we want to keep the current value in the class
        # variable. (A failure in reading a parameter from the local current
        # process config can happen when the parameter exists, but has an unexpected
        # type.) In case of such a failure, we get None for that parameter;
        # so we have to handle the case of a parameter being None. Finally,
        # we will return True if there is no None parameter and False otherwise.
        params = list(self.__get_process_config_params().values())
        sleep_time, max_jobs_queued, max_jobs_submit, submit_wait_time, \
        drain, cancel \
            = params[:]
        if log_changes:
            if sleep_time is not None and sleep_time != self.sleep_time:
                logging.info("Dag config change detected: sleep_time set to %s seconds" % (sleep_time))
            if max_jobs_queued is not None and max_jobs_queued != self.max_jobs_queued:
                logging.info("Dag config change detected: max_jobs_queued set to %s" % (max_jobs_queued))
            if max_jobs_submit is not None and max_jobs_submit != self.max_jobs_submit:
                logging.info("Dag config change detected: max_jobs_submit set to %s" % (max_jobs_submit))
            if submit_wait_time is not None and submit_wait_time != self.submit_wait_time:
                logging.info("Dag config change detected: submit_wait_time set to %s" % (submit_wait_time))
            if drain is not None and drain != self.drain:
                logging.info("Dag config change detected: drain set to %s" % (drain))
            if cancel is not None and cancel != self.cancel:
                logging.info("Dag config change detected: cancel set to %s" % (cancel))
        if sleep_time is not None:
            self.sleep_time = sleep_time
        if max_jobs_queued is not None:
            self.max_jobs_queued = max_jobs_queued
        if max_jobs_submit is not None:
            self.max_jobs_submit = max_jobs_submit
        if submit_wait_time is not None:
            self.submit_wait_time = submit_wait_time
        if drain is not None:
            self.drain = drain
        if cancel is not None:
            self.cancel = cancel
        return None not in params


    def __handle_process_config_loading_parsing_and_writing(self):
        rewrite = False
        self.__reset_process_config()
        if not self.__load_process_config():
            rewrite = True 
        elif not self.__parse_process_config() or not self.__validate_process_config():
            self.__reset_process_config()
            rewrite = True
        if rewrite:
            self.__set_process_config_params()
            self.__write_process_config()


    def __parse_dag_file(self):
        self.dag.parse()


    def __pre_execute_dag(self):
        nodes_all = self.dag.get_nodes()
        self.num_nodes_total = len(nodes_all)
        self.num_nodes_unready = len(nodes_all)
        for node in nodes_all:
            if self.dag[node]['done']:
                self.dag_done[node] = copy.deepcopy(self.dag[node])
                self.dag.__dict__().pop(node)
        del nodes_all
        nodes_done = self.dag_done.keys()
        self.num_nodes_done = len(nodes_done)
        self.num_nodes_unready -= self.num_nodes_done
        nodes_not_done = self.dag.get_nodes()
        for node in nodes_not_done:
            self.dag[node]['status'] = 0
            self.dag_done[node] = {'parents': []}
            parents = copy.copy(self.dag[node]['parents'])
            for parent in parents:
                if parent in nodes_done:
                    if parent not in self.dag_done[node]['parents']:
                        self.dag_done[node]['parents'].append(parent)
                    self.dag[node]['parents'].remove(parent)
            if not self.dag[node]['parents']:
                self.__mark_node_as_ready(node)


    def __pre_write_dag(self):
        for node in list(self.dag_done.keys()):
            if node not in self.dag:
                self.dag[node] = copy.deepcopy(self.dag_done[node])
            else:
                for parent in self.dag_done[node]['parents']:
                    if parent not in self.dag[node]['parents']:
                        self.dag[node]['parents'].append(parent)
            self.dag_done.pop(node)


    def write_rescue_dag_file(self):
        if self.num_nodes_done > 0:
            rescue_dag_file = build_next_rescue_dag_file_name(self.dag.get_dag_file())
            logging.info('Writing rescue dag file %s ...' % (rescue_dag_file))
            self.__write_dag_file(rescue_dag_file, True)
            logging.info('Rescue dag file saved.')


    def __write_dag_file(self, dag_file, add_done_labels=True):
        self.__pre_write_dag()
        self.dag.write(dag_file, use_dag_nodes_appearance_order=True, add_done_labels=add_done_labels)


    def __submit_ready_nodes(self):
        num_submitted_nodes = 0
        for node in self.dag:
            if self.max_jobs_queued > 0 and len(self.queued_job_ids) >= self.max_jobs_queued:
                break
            if self.dag[node]['status'] == 1: # node ready to be submitted
                # Submit a job
                job_id, error = self.__submit(self.dag[node]['job_submission_file'], node)
                if job_id is not None:
                    if 'retry_num' in self.dag[node] and self.dag[node]['retry_num'] > 0:
                        logging.info('Submitted node %s (retry number %i out of %i): %s' % (node, self.dag[node]['retry_num'], self.dag.get_max_retries(node), job_id))
                    else:
                        logging.info('Submitted node %s: %s' % (node, job_id))
                    self.__mark_node_as_queued(node, job_id)
                    self.queued_job_ids.append(job_id)
                    num_submitted_nodes += 1
                else:
                    self.__mark_node_as_queued(node)
                    if 'retry_num' in self.dag[node] and self.dag[node]['retry_num'] > 0:
                        logging.error('Failed to submit node %s (retry number %i out of %i)' % (node, self.dag[node]['retry_num'], self.dag.get_max_retries(node)))
                    else:
                        logging.error('Failed to submit node %s' % (node))
                    if 'retry_num' in self.dag[node] and self.dag[node]['retry_num'] < self.dag.get_max_retries(node):
                        self.__mark_node_as_ready(node)
                        logging.info('Node %s will be retried.' % (node))
                    else:
                        self.__mark_node_as_failed(node)
                    if error is not None:
                        logging.debug('Error was: %s' % (error))
                if self.max_jobs_submit > 0 and num_submitted_nodes >= self.max_jobs_submit:
                    break
                if self.submit_wait_time > 0:
                    time.sleep(self.submit_wait_time)


    def __submit(self, job_submission_file, node):
        with open(job_submission_file + '.tmp', 'w') as fd1:
            with open(job_submission_file, 'r') as fd2:
                for line in fd2:
                    fd1.write(self.__replace_vars(line, node))
        cmd = ['sbatch', '--job-name=%s' % (node), '--wckey=%s' % (self.wckey), job_submission_file + '.tmp']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.decode('utf-8').strip()
        err = err.decode('utf-8').strip()
        os.remove(job_submission_file + '.tmp')
        if err or not out:
            return None, err
        job_id = out.strip('\n').split()[-1]
        return job_id, None


    def __handle_proxy(self):
        if self.proxy is not None:
            destdir = os.path.expanduser('~')
            if not os.path.exists(destdir):
                os.mkdir(destdir)
            elif not os.path.isdir(destdir):
                raise OSError('Error: %s exists and it is not a directory.' % (destdir)) 
            shutil.copy(self.proxy, os.path.join(destdir, '.x509-user-proxy'))
            self.proxy = os.path.join(destdir, '.x509-user-proxy')
            os.environ['X509_USER_PROXY'] = self.proxy


    def __replace_vars(self, string, node):
        startindex = 0
        while True:
            i = string.find('$(', startindex)
            j = string.find(')', max(startindex, i))
            startindex = j+1
            if not (i > -1 and j > i):
                break
            macrokey = string[i+2:j]
            m = self.dag[node].get('vars', '').split(macrokey+'="')
            if len(m) == 2:
                n = m[1].find('"')
                if n > -1:
                    macrovalue = m[1][:n]
                    string = string[:i] + macrovalue + string[j+1:]
                    startindex += (len(macrovalue) - (len(macrokey) + 3))
                else:
                    string = string[:i] + string[j+1:]
                    startindex -= (len(macrokey) + 3)
            else:
                string = string[:i] + string[j+1:]
                startindex -= (len(macrokey) + 3)
        return string


    def __sacct(self):
        cmd = ['sacct', '--noheader', '-P', '--wckeys=%s' % (self.wckey), '--format=JobID,JobName,State,NodeList,ExitCode', '--jobs=%s' % (','.join(self.queued_job_ids)), '--starttime=%s' % (self.start_time)]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.decode('utf-8').strip().split('\n')
        err = err.decode('utf-8').strip().split('\n')
        out = [l for l in out if l and l.find('|batch|') == -1 and l.find('|extern|') == -1]
        if len(err) == 1 and err[0] == '': err = []
        return out, err


    def __squeue(self):
        cmd = ['squeue', '--noheader', '--format="%i|%T|%w|%N"']
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.decode('utf-8').strip().split('\n')
        err = err.decode('utf-8').strip().split('\n')
        out = [l for l in out if l and l.find('|%s|' % (self.wckey)) > 0]
        if len(err) == 1 and err[0] == '': err = []
        return out, err


    def __monitor(self):
        num_nodes_running = 0
        num_nodes_pending = 0
        num_nodes_unknown = 0
        sacct_out, _ = self.__sacct()
        squeue_out, _ = self.__squeue()
        squeue_result = {}
        for job in squeue_out:
            job_id, status, wckey, computing_node = job.split('|')
            squeue_result[job_id] = {'status': status, 'computing_node': computing_node}
        nodes_done = set()
        for job in sacct_out:
            job_id, job_name, status, computing_node, exit_code = job.split('|')
            try:
                exit_code = int(exit_code.split(':')[0])
            except ValueError:
                exit_code = 'undefined'
            if job_id in squeue_result:
                status = squeue_result[job_id]['status']
                computing_node = squeue_result[job_id]['computing_node']
            #if self.dag[job_name]['status'] == 2:
            if status == 'PENDING':
                num_nodes_pending += 1
            elif status in ['RUNNING', 'COMPLETING']:
                num_nodes_running += 1
            elif status == 'COMPLETED':
                logging.info('Node %s completed' % (job_name))
                self.__mark_node_as_done(job_name)
                nodes_done.add(job_name)
                self.queued_job_ids.remove(job_id)
            elif status in ['RESIZING', 'REQUEUED', 'REVOKED', 'SUSPENDED']:
                num_nodes_unknown += 1
            else:
                if status == 'FAILED':
                    logging.info('Node %s failed (exit code %s)' % (job_name, exit_code))
                else:
                    logging.info('Node %s in status %s (exit_code %s) assumed to have failed' % (job_name, status, exit_code))
                if 'retry_num' in self.dag[job_name] and self.dag[job_name]['retry_num'] < self.dag.get_max_retries(job_name) and exit_code not in self.dag.get_no_retry_exit_codes(job_name):
                    self.__mark_node_as_ready(job_name)
                    logging.info('Node %s will be retried' % (job_name))
                else:
                    self.__mark_node_as_failed(job_name)
                self.queued_job_ids.remove(job_id)
        if nodes_done:
            self.__fix_parents(nodes_done)
        return num_nodes_running, num_nodes_pending, num_nodes_unknown


    def __fix_parents(self, nodes_done):
        for node in self.dag:
            if self.dag[node]['status'] == 0:
                for node_done in nodes_done:
                    if node_done in self.dag[node]['parents']:
                        if node not in self.dag_done: 
                            self.dag_done[node] = {'parents': []}
                        self.dag_done[node]['parents'].append(node_done)
                        self.dag[node]['parents'].remove(node_done)
                        if not self.dag[node]['parents']:
                            self.__mark_node_as_ready(node)
                            break


    def __mark_node_as_ready(self, node):
        self.dag[node]['status'] = 1
        self.num_nodes_ready += 1
        if 'retry_num' in self.dag[node]:
            self.dag[node]['retry_num'] += 1
            if self.dag[node]['retry_num'] > 0:
                self.num_nodes_queued -= 1
            else:
                self.num_nodes_unready -= 1
        else:
            self.num_nodes_unready -= 1


    def __mark_node_as_queued(self, node, job_id=None):
        if job_id is not None:
            self.dag[node]['job_id'] = job_id
        self.dag[node]['status'] = 2
        self.num_nodes_queued += 1
        self.num_nodes_ready -= 1


    def __mark_node_as_done(self, node):
        self.dag[node]['status'] = 3
        self.dag[node]['done'] = True
        self.num_nodes_done += 1
        self.num_nodes_queued -= 1
        for parent in self.dag_done[node]['parents']:
            if parent not in self.dag[node]['parents']:
                self.dag[node]['parents'].append(parent)
        self.dag_done.pop(node)
        self.dag_done[node] = copy.deepcopy(self.dag[node])
        self.dag.__dict__().pop(node)


    def __mark_node_as_failed(self, node):
        self.dag[node]['status'] = 4
        self.num_nodes_failed += 1
        self.num_nodes_queued -= 1


    def __execute_dag(self):
        self.__pre_execute_dag()
        logging.info('******************************************************')
        logging.info('** PID = %s' % (os.getpid()))
        if os.getppid():
            logging.info('** Parent PID = %s' % (os.getppid()))
        logging.info('******************************************************')
        logging.info('DAG file: %s' % (self.dag.get_dag_file()))
        logging.info('DAGMan config file for this DAG: %s' % (self.process_config_file))
        logging.info('Sleep time between iterations: %s secs' % (self.sleep_time))
        logging.info('Will submit jobs with wckey=%s' % (self.wckey))
        while True:
            num_nodes_running, num_nodes_pending, num_nodes_unknown = self.__monitor()
            logging.info('Of %i nodes total:' % (self.num_nodes_total))
            logging.info('  Done   Queued    Ready   Un-Ready   Failed')
            logging.info('   ===      ===      ===        ===      ===')
            logging.info('%6s   %6s   %6s     %6s   %6s' % (self.num_nodes_done, self.num_nodes_queued, self.num_nodes_ready, self.num_nodes_unready, self.num_nodes_failed))
            if self.num_nodes_queued > 0:
                logging.info('Of %i nodes queued: %i running, %i pending, %i other' % (self.num_nodes_queued, num_nodes_running, num_nodes_pending, num_nodes_unknown))
            if self.num_nodes_ready > 0 and not self.drain and not self.cancel:
                self.__submit_ready_nodes()
            else:
                if self.num_nodes_done == self.num_nodes_total:
                    logging.info('DAG completed successfully.')
                    return 0
                elif self.num_nodes_queued == 0:
                    if self.num_nodes_failed:
                        logging.info('DAG failed.')
                        return 1
                    else:
                        logging.info('DAG stopped.')
                        return 2
            if self.cancel:
                return 2
            if self.sleep_time > 0:
                time.sleep(self.sleep_time)
            self.__handle_process_config_loading_parsing_and_writing()


    def __cancel_dag(self):
        max_num_cancel_retries = 5
        cancel_retry_num = 1
        while True:
            cmd = ['scancel', '--wckey=%s' % (self.wckey)]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            time.sleep(60)
            squeue_out, squeue_err = self.__squeue()
            if squeue_out or squeue_err:
                if cancel_retry_num > max_num_cancel_retries:
                    msg  = 'After %i trials to cancel the queued DAG nodes, %i nodes are still queued.' % (cancel_retry_num, len(squeue_out))
                    msg += ' Aborting the cancel.'
                    msg += ' This is the output of squeue:'
                    msg += '\nStdout:\n%s' % ('\n'.join(squeue_out))
                    if squeue_err:
                        msg += '\nStderr:\n%s' % ('\n'.join(squeue_err))
                    raise Exception(msg)
                cancel_retry_num += 1
            else:
                break


    def run(self):
        self.__handle_proxy()
        try:
            self.__parse_dag_file()
        except Exception as e:
            logging.exception('Failure parsing DAG file.')
            raise
        try:
            rc = self.__execute_dag()
        except Exception as e:
            logging.exception('Failure executing DAG.')
            raise
        if self.cancel and rc != 0:
            self.try_to_terminate_and_write_rescue_dag_file()
            rc = -1
        return rc


    def terminate(self):
        if self.num_nodes_queued > 0:
            try:
                logging.info('Cancelling queued DAG nodes.')
                self.__cancel_dag()
                logging.info('Queued DAG nodes cancelled.')
            except Exception as e:
                logging.exception('Failure cancelling queued DAG nodes.')
                raise


    def try_to_terminate_and_write_rescue_dag_file(self):
        try:
            self.terminate()
        except Exception:
            logging.warning('Failed to terminate DAG. Will write rescue DAG file with currently finished nodes.')
            pass
        try:
            self.write_rescue_dag_file()
        except Exception:
            logging.exception('Failed to write rescue DAG.')
            raise
