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

import math
import os

from SlurmDagman.config.data.package import package_config
from SlurmDagman.dag import Dag


class CondorToSlurmTranslator(object):

    def __init__(self, condor_dag_file=None, slurm_dag_file=None,
                       slurm_partition=None, slurm_qos=None, slurm_time_limit=None, slurm_scratch_dir=None, slurm_use_setsid=None,
                       singularity_image=None, singularity_bind_mount_dbus=None):
        super(CondorToSlurmTranslator, self).__init__()
        self.condor_default_cpus = '1'
        self.condor_default_memory = '1GB'
        self.__init_dags(condor_dag_file, slurm_dag_file)
        self.__init_params()
        self.set_params(slurm_partition, slurm_qos, slurm_time_limit, slurm_scratch_dir, slurm_use_setsid,
                        singularity_image, singularity_bind_mount_dbus)


    def __init_dags(self, condor_dag_file=None, slurm_dag_file=None):
        self.__init_condor_dag(condor_dag_file)
        self.__init_slurm_dag(slurm_dag_file)


    def __init_condor_dag(self, condor_dag_file=None):
        self.condor_dag = Dag()
        if condor_dag_file:
            self.set_condor_dag_file(condor_dag_file)


    def __init_slurm_dag(self, slurm_dag_file=None):
        self.slurm_dag = Dag()
        if slurm_dag_file:
            self.set_slurm_dag_file(slurm_dag_file)


    def reset_dags(self):
        self.reset_condor_dag()
        self.reset_slurm_dag()


    def reset_condor_dag(self):
        self.__init_condor_dag()


    def reset_slurm_dag(self):
        self.__init_slurm_dag()


    def set_condor_dag_file(self, condor_dag_file):
        self.condor_dag.set_dag_file(condor_dag_file)


    def set_slurm_dag_file(self, slurm_dag_file):
        self.slurm_dag.set_dag_file(slurm_dag_file)


    def __init_params(self):
        self.slurm_partition = package_config.get_param('SLURM', 'partition')
        self.slurm_qos = package_config.get_param('SLURM', 'qos')
        self.slurm_time_limit = package_config.get_param('SLURM', 'time_limit')
        self.slurm_scratch_dir = package_config.get_param('SLURM', 'scratch_dir')
        self.slurm_use_setsid = False
        self.singularity_image = ''
        self.singularity_bind_mount_dbus = False


    def reset_params(self):
        self.__init_params()


    def set_params(self, slurm_partition=None, slurm_qos=None, slurm_time_limit=None, slurm_scratch_dir=None, slurm_use_setsid=None,
                         singularity_image=None, singularity_bind_mount_dbus=None):
        if slurm_partition is not None:
            self.slurm_partition = slurm_partition
        if slurm_qos is not None:
            self.slurm_qos = slurm_qos
        if slurm_time_limit is not None:
            self.slurm_time_limit = slurm_time_limit
        if slurm_scratch_dir is not None:
            self.slurm_scratch_dir = slurm_scratch_dir
        if slurm_use_setsid is not None:
            self.slurm_use_setsid = slurm_use_setsid
        if singularity_image is not None:
            self.singularity_image = singularity_image
        if singularity_bind_mount_dbus is not None:
            self.singularity_bind_mount_dbus = singularity_bind_mount_dbus


    def translate(self):
        self.translate_dag_file()
        self.translate_job_submission_files()


    def translate_dag_file(self):
        self.__parse_condor_dag_file()
        self.__condor_to_slurm_dag()
        self.__write_slurm_dag_file()


    def __parse_condor_dag_file(self):
        self.condor_dag.parse()


    def __condor_to_slurm_dag(self):
        self.slurm_dag.set_dag(self.condor_dag, True)
        for node in self.slurm_dag:
            self.slurm_dag[node]['job_submission_file'] = self.condor_dag[node]['job_submission_file'] + '.slurm'
            self.slurm_dag[node]['vars'] = self.slurm_dag[node]['vars'].replace('_CONDOR_SCRATCH_DIR', self.slurm_scratch_dir)


    def __write_slurm_dag_file(self):
        self.slurm_dag.write(use_dag_nodes_appearance_order=True)


    def translate_job_submission_files(self):
        condor_job_submission_files = []
        for dag_node in self.condor_dag:
            condor_job_submission_file = self.condor_dag[dag_node]['job_submission_file']
            if condor_job_submission_file not in condor_job_submission_files:
                condor_job_description = self.__parse_condor_job_submission_file(condor_job_submission_file)
                slurm_job_description = self.__condor_to_slurm_job_description(condor_job_description, dag_node)
                self.__write_slurm_job_submission_file(slurm_job_description, self.slurm_dag[dag_node]['job_submission_file'])
                condor_job_submission_files.append(condor_job_submission_file)


    def __parse_condor_job_submission_file(self, condor_job_submission_file):
        condor_job_description = {
            'executable': '',
            'arguments': '',
            'memoryusage': '',
            'memory': '',
            'cpus': '',
            'environment': [],
            'output': '',
            'error': '',
        }
        with open(condor_job_submission_file, 'r') as fd:
            for line in fd:
                if line.startswith('executable = '):
                    condor_job_description['executable'] = line.split(' = ' )[1].strip()
                elif line.startswith('arguments = '):
                    condor_job_description['arguments'] = line.strip('\n').replace('" ', '').replace(' "', '')[len('arguments = '):]
                elif line.startswith('+MemoryUsage = '):
                    condor_job_description['memoryusage'] = line.split(' = ')[1].strip('\n')
                elif line.startswith('request_memory = '):
                    condor_job_description['memory'] = line.split(' = ')[1].replace('MemoryUsage', condor_job_description['memoryusage']).strip('\n').replace('* 2 / 3 ) * 3 / 2', ')')
                elif line.startswith('request_cpus = '):
                    condor_job_description['cpus'] = line.split(' = ')[1].strip()
                elif line.startswith('environment = '):
                    condor_job_description['environment'] = line.strip('\n').replace('"', '').replace(';', '')[len('environment = '):].split()
                elif line.startswith('output = '):
                    condor_job_description['output'] = line.split(' = ')[1].strip()
                elif line.startswith('error = '):
                    condor_job_description['error'] = line.split(' = ')[1].strip()
        if condor_job_description['executable'] == '':
            msg  = 'Error parsing condor job submission file %s.\n' % (condor_job_submission_file)
            msg += 'No executable line found.'
            raise SyntaxError(msg)
        if condor_job_description['cpus'] == '':
            condor_job_description['cpus'] = self.condor_default_cpus
        if condor_job_description['memory'] == '':
            condor_job_description['memory'] = self.condor_default_memory
        return condor_job_description


    def __condor_to_slurm_job_description(self, condor_job_description, dag_node):
        slurm_job_description = {
            'cpus': condor_job_description['cpus'],
            'memory_per_cpu': self.__condor_to_slurm_memory(condor_job_description['memory'], condor_job_description['cpus']),
            'time': self.slurm_time_limit,
            'environment': self.__condor_to_slurm_environment(condor_job_description['environment'], dag_node),
            'output': self.__condor_to_slurm_output_file(condor_job_description['output']),
            'error': self.__condor_to_slurm_error_file(condor_job_description['error']),
            'payload': self.__condor_to_slurm_payload(condor_job_description['executable'], condor_job_description['arguments']),
        }
        return slurm_job_description


    def __condor_to_slurm_memory(self, condor_memory, cpus):
        memory = condor_memory
        if memory.endswith('MB'):
            memory = memory.replace('MB', '')
        elif memory.endswith('GB'):
            memory = memory.replace('GB', '') + ' * 1024'
        memory = int(eval(memory))
        memory_per_cpu = int(math.ceil(int(memory)/int(cpus)))
        return str(memory_per_cpu)


    def __condor_to_slurm_environment(self, condor_environment, dag_node):
        environment = 'ALL'
        for env in condor_environment:
            var, val = env.split('=')
            environment += ',%s' % (env)
        return environment


    def __condor_to_slurm_output_file(self, condor_output_file):
        if condor_output_file:
            output_file = condor_output_file.replace('$(cluster)', '%j').replace('-$(process)', '')
        else:
            output_file = 'slurm-%j.out'
        return output_file


    def __condor_to_slurm_error_file(self, condor_error_file):
        if condor_error_file:
            error_file = condor_error_file.replace('$(cluster)', '%j').replace('-$(process)', '')
        else:
            error_file = 'slurm-%j.err'
        return error_file


    def __condor_to_slurm_payload(self, condor_executable, condor_arguments):
        command = '%s %s' % (condor_executable, condor_arguments)
        if self.singularity_image:
            payload = \
"""\
export SINGULARITYENV_TMPDIR=${TMPDIR}
SINGULARITY_IMAGE=__SINGULARITY_IMAGE__
CMD="__COMMAND__"
# Passing the command to singularity via a pipe is to avoid this issue: https://github.com/hpcng/singularity/issues/5057
echo "${CMD}" | __SINGULARITY_COMMAND__\
"""
            singularity_command = 'singularity exec __SINGULARITY_OPTIONS__ ${SINGULARITY_IMAGE} bash -c "read -u 0 line; set -- \$line; exec \"\$@\""'
            if self.slurm_use_setsid:
                singularity_command = 'setsid ' + singularity_command
            bind_mount_dirs = ['${TMPDIR}']
            if self.singularity_bind_mount_dbus:
                bind_mount_dirs += ['/run/dbus', '/var/run/dbus']
            bind_mounts_opts_str = '-B ' + ' -B '.join(bind_mount_dirs)
            singularity_command = singularity_command.replace('__SINGULARITY_OPTIONS__', bind_mounts_opts_str)
            payload = payload \
                          .replace('__SINGULARITY_IMAGE__', self.singularity_image) \
                          .replace('__COMMAND__', command) \
                          .replace('__SINGULARITY_COMMAND__', singularity_command)
        else:
            payload = command
            if self.slurm_use_setsid:
                payload = 'setsid ' + payload
        return payload


    def __write_slurm_job_submission_file(self, slurm_job_description, slurm_job_submission_file):
        sbatch_directives_template = \
"""\
#SBATCH --cpus-per-task=__CPUS__
#SBATCH --mem-per-cpu=__MEMORY_PER_CPU__
#SBATCH --time=__TIME__
#SBATCH --export=__ENVIRONMENT__
#SBATCH --no-requeue
#SBATCH --output=__OUTPUT__
#SBATCH --error=__ERROR__\
"""
        sbatch_directives = sbatch_directives_template \
                      .replace('__CPUS__', slurm_job_description['cpus']) \
                      .replace('__MEMORY_PER_CPU__', slurm_job_description['memory_per_cpu']) \
                      .replace('__TIME__', slurm_job_description['time']) \
                      .replace('__ENVIRONMENT__', slurm_job_description['environment']) \
                      .replace('__OUTPUT__', slurm_job_description['output']) \
                      .replace('__ERROR__', slurm_job_description['error'])
        if self.slurm_partition:
            sbatch_directives += "\n#SBATCH --partition=%s" % (self.slurm_partition)
        if self.slurm_qos:
            sbatch_directives += "\n#SBATCH --qos=%s" % (self.slurm_qos)
        content_template = \
"""\
#!/bin/bash
#
__SBATCH_DIRECTIVES__

echo "======== Starting job (`date`) ========"
echo "Job id: ${SLURM_JOB_ID}"
echo "Job name: ${SLURM_JOB_NAME}"
echo "List of nodes allocated to the job: ${SLURM_JOB_NODELIST}"
echo "Batch node: ${SLURMD_NODENAME}"
echo "Working directory: `pwd`"

echo "==== Starting execution of payload (`date`) ===="
echo "------------------------- Begin payload output ------------------------"
(
__PAYLOAD__
)
pec=$?
echo "--------------------------- End payload output ------------------------"
echo "Payload exit code: ${pec}"
echo "==== Finished execution of payload (`date`) ===="

echo "======== Finished job (`date`) ========"
echo "Batch script exit code: ${pec}"
exit ${pec}
"""
        content = content_template \
                      .replace('__SBATCH_DIRECTIVES__', sbatch_directives) \
                      .replace('__PAYLOAD__', slurm_job_description['payload'])
        with open(slurm_job_submission_file, 'w') as fd:
            fd.write(content)
