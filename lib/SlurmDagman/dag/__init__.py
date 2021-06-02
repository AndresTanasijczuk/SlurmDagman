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


class Dag(object):

    def __init__(self, dag_file=''):
        super(Dag, self).__init__()
        self.dag_file = dag_file.strip()
        self.dag_nodes_appearance_order = {}
        self.dag = {}
        self.empty_node_template = {'job_submission_file': '', 'parents': [], 'done': False}
        self.max_retries = None
        self.no_retry_exit_codes = [0]


    def get_dag_file(self):
        return self.dag_file


    def set_dag_file(self, dag_file):
        self.dag_file = dag_file.strip()


    def set_dag(self, dag, copy_dag_nodes_appearance_order=False):
        if copy_dag_nodes_appearance_order and isinstance(dag, Dag):
            self.dag_nodes_appearance_order = copy.copy(dag.dag_nodes_appearance_order)
        for node in dag:
            self.dag[node] = copy.deepcopy(dag[node])


    def reset(self):
        for node in self.dag:
            self.reset_node(node)


    def reset_node(self, node):
        self.dag[node] = copy.deepcopy(self.empty_node_template)


    def get_nodes(self):
        return list(self.dag.keys())


    def parse(self):
        with open(self.dag_file, 'r') as fd:
            lines = fd.readlines()
            # Make a first pass to get all the dag nodes.
            for i, line in enumerate(lines):
                linestrip = line.strip()
                if linestrip.startswith('JOB '):
                    items = linestrip.split()
                    if len(items) < 3 or len(items) > 4 or (len(items) == 4 and items[3] != 'DONE'):
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Unexpected line format.\n"
                        msg += "Expected line format:\n"
                        msg += "JOB <node> <job-submission-file> [DONE]"
                        raise SyntaxError(msg)
                    node = items[1]
                    if node in self.dag_nodes_appearance_order:
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Node '%s' was already defined in line %i." % (node, self.dag_nodes_appearance_order[node])
                        raise SyntaxError(msg)
                    self.dag_nodes_appearance_order[node] = i
                    self.reset_node(node)
                    self.dag[node]['job_submission_file'] = items[2]
                    if len(items) == 4:
                        self.dag[node]['done'] = True
            # Make a second pass to find all the needed information about the dag nodes.
            for i, line in enumerate(lines):
                linestrip = line.strip()
                if linestrip.startswith('VARS '):
                    items = linestrip.split()
                    if len(items) < 3:
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Unexpected line format.\n"
                        msg += "Expected line format:\n"
                        msg += "VARS <node> <macro-1-key>=\"<macro-1-value>\" [<macro-2-key>=\"<macro-2-value>\" ...]\n"
                        msg += "(where macro values are allowed to contain white spaces)"
                        raise SyntaxError(msg)
                    node = items[1]
                    if node not in self.dag:
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Found a VARS line for node '%s', but there is no JOB line for this node." % (node)
                        raise SyntaxError(msg)
                    if 'vars' in self.dag[node]:
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Found a second VARS line for node '%s'.\n" % (node)
                        msg += "Only one VARS line can be specified per node."
                        raise SyntaxError(msg)
                    self.dag[node]['vars'] = ' '.join(items[2:])
                elif linestrip.startswith('PARENT '):
                    j = linestrip.find(' CHILD ')
                    if j == -1:
                        msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                        msg += "Unexpected line format.\n"
                        msg += 'Expected line format:\n'
                        msg += 'PARENT <parent-node-1> [<parent-node-2> ...] CHILD <child-node-1> [<child-node-2> ...]'
                        raise SyntaxError(msg)
                    parents = linestrip[:j][len('PARENT '):].replace(',', ' ').split()
                    children = linestrip[j+len(' CHILD '):].replace(',', ' ').split()
                    for parent in parents:
                        if parent not in self.dag:
                            msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                            msg += "There is no JOB line for parent node '%s'." % (parent)
                            raise SyntaxError(msg)
                    for child in children:
                        if child not in self.dag:
                            msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                            msg += "There is no JOB line for child node '%s'." % (child)
                            raise SyntaxError(msg)
                        for parent in parents:
                            if parent not in self.dag[child]['parents']:
                                self.dag[child]['parents'].append(parent)
                elif linestrip.startswith('RETRY '):
                    items = linestrip.split()
                    wrong_line_format_msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                    wrong_line_format_msg += "Unexpected line format.\n"
                    wrong_line_format_msg += "Expected line format:\n"
                    wrong_line_format_msg += "RETRY [<node> | ALL_NODES] <max-retries> [UNLESS-EXIT <exit-code1>[,<exit-code2>...]]\n"
                    wrong_line_format_msg += "(with max-retries a non-negative integer value and exit-code an integer value)"
                    if len(items) not in [3, 5]:
                        raise SyntaxError(wrong_line_format_msg)
                    try:
                        max_retries = int(items[2])
                        if max_retries < 0:
                            raise ValueError
                    except ValueError:
                        raise SyntaxError(wrong_line_format_msg)
                    if len(items) == 5:
                        if items[3] != 'UNLESS-EXIT':
                            raise SyntaxError(wrong_line_format_msg)
                        try:
                            no_retry_exit_codes = [int(i) for i in items[4].split(',')]
                        except ValueError:
                            raise SyntaxError(wrong_line_format_msg)
                    if items[1] == 'ALL_NODES':
                        if self.max_retries is not None:
                            msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                            msg += "Found a second RETRY ALL_NODES line.\n"
                            msg += "Only one RETRY ALL_NODES line can be specified."
                            raise SyntaxError(msg)
                        self.max_retries = max_retries
                        for node in self.dag:
                            self.dag[node].pop('max_retries', None)
                            if max_retries > 0:
                                self.dag[node]['retry_num'] = -1
                            else:
                                self.dag[node].pop('retry_num', None)
                        if len(items) == 5:
                            self.no_retry_exit_codes = no_retry_exit_codes
                            for node in self.dag:
                                self.dag[node].pop('no_retry_exit_codes', None)
                    else:
                        node = items[1]
                        if node not in self.dag:
                            msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                            msg += "Found a RETRY line for node '%s', but there is no JOB line for this node." % (node)
                            raise SyntaxError(msg)
                        if 'max_retries' in self.dag[node]:
                            msg  = "Error parsing dag file %s line %i.\n" % (self.dag_file, i)
                            msg += "Found a second RETRY line for node '%s'.\n" % (node)
                            msg += "Only one RETRY line can be specified per node."
                            raise SyntaxError(msg)
                        self.dag[node]['max_retries'] = max_retries
                        if max_retries > 0:
                            self.dag[node]['retry_num'] = -1
                        else:
                            self.dag[node].pop('retry_num', None)
                        if len(items) == 5:
                            self.dag[node]['no_retry_exit_codes'] = no_retry_exit_codes


    def get_max_retries(self, node):
        return self.dag[node].get('max_retries', self.max_retries)


    def get_no_retry_exit_codes(self, node):
        return self.dag[node].get('no_retry_exit_codes', self.no_retry_exit_codes)


    def write(self, dag_file='', use_dag_nodes_appearance_order=False, add_done_labels=False):
        dag_file = dag_file.strip()
        if dag_file == '':
            if self.dag_file != '':
                dag_file = self.dag_file
            else:
                raise ValueError('Dag file not specified.')
        if use_dag_nodes_appearance_order and self.dag_nodes_appearance_order:
            nodes = sorted(self.dag_nodes_appearance_order, key=self.dag_nodes_appearance_order.get)
        else:
            nodes = self.get_nodes()
        jobs_lines = []
        for node in nodes:
            job_line = 'JOB %s %s' % (node, self.dag[node]['job_submission_file'])
            if add_done_labels and self.dag[node]['done']:
                job_line += ' DONE'
            jobs_lines.append(job_line)
            if self.dag[node].get('vars', ''):
                vars_line = 'VARS %s %s' % (node, self.dag[node]['vars'])
                jobs_lines.append(vars_line)
            if 'max_retries' in self.dag[node]:
                retry_line = 'RETRY %s %i' % (node, self.dag[node]['max_retries'])
                jobs_lines.append(retry_line)
        dependency_lines = []
        for node in nodes:
            for parent in self.dag[node]['parents']:
                dependency_lines.append('PARENT %s CHILD %s' % (parent, node))
        retry_all_nodes_line = ['RETRY ALL_NODES %i' % (self.max_retries)] if self.max_retries is not None else [] 
        content = '\n'.join(jobs_lines + dependency_lines + retry_all_nodes_line)
        with open(dag_file, 'w') as fd:
            fd.write(content)


    def __str__(self):
        return str(self.dag)


    def __iter__(self):
        return iter(self.dag)


    def __next__(self, node):
        if node in self.dag:
            return self.dag[node]
        else:
            raise StopIteration


    def __getitem__(self, node):
        return self.dag[node]


    def __setitem__(self, node, value):
        self.dag[node] = value


    def __dict__(self):
        return self.dag
