#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import os

from idds.common.constants import (ProcessingStatus)
from idds.common.utils import run_command
from idds.workflow.work import Work


class ATLASCondorWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_type=None, work_tag='hpo', exec_type='local', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None, input_collections=None,
                 primary_output_collection=None, other_output_collections=None,
                 output_collections=None, log_collections=None,
                 agent_attributes=None,
                 logger=None):
        """
        Init a work/task/transformation.

        :param setup: A string to setup the executable enviroment, it can be None.
        :param executable: The executable.
        :param arguments: The arguments.
        :param parameters: A dict with arguments needed to be replaced.
        :param work_type: The work type like data carousel, hyperparameteroptimization and so on.
        :param exec_type: The exec type like 'local', 'remote'(with remote_package set), 'docker' and so on.
        :param sandbox: The sandbox.
        :param work_id: The work/task id.
        :param primary_input_collection: The primary input collection.
        :param other_input_collections: List of the input collections.
        :param output_collections: List of the output collections.
        # :param workflow: The workflow the current work belongs to.
        """
        super(ATLASCondorWork, self).__init__(executable=executable, arguments=arguments,
                                              parameters=parameters, setup=setup, work_type=work_type,
                                              exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                              primary_input_collection=primary_input_collection,
                                              other_input_collections=other_input_collections,
                                              primary_output_collection=primary_output_collection,
                                              other_output_collections=other_output_collections,
                                              input_collections=input_collections,
                                              output_collections=output_collections,
                                              log_collections=log_collections,
                                              agent_attributes=agent_attributes,
                                              logger=logger)

    def get_long_id(self, processing):
        request_id = processing['request_id']
        workload_id = processing['workload_id']
        processing_id = processing['processing_id']
        long_id = '%s_%s_%s' % (request_id, workload_id, processing_id)
        return long_id

    def get_working_dir(self, processing):
        # request_id = processing['request_id']
        # workload_id = processing['workload_id']
        processing_id = processing['processing_id']

        job_dir = 'processing_%s' % (processing_id)
        job_dir = os.path.join(self.get_workdir(), job_dir)
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        return job_dir

    def generate_processing_submit_file(self, processing):
        script_name, err_msg = self.generate_processing_script(processing)
        if not script_name:
            return None, err_msg

        input_files = self.get_input_files(processing)
        output_files = self.get_output_files(processing)
        # self.logger.info("input_files: %s, output_files: %s" % (str(input_files), str(output_files)))

        long_id = self.get_long_id(processing)

        jdl = "#Agent jdl file\n"
        jdl += "Universe        = vanilla\n"
        jdl += "Notification    = Never\n"
        jdl += "initialdir      = %s\n" % self.get_working_dir(processing)
        jdl += "Executable      = %s\n" % script_name
        # jdl += "Arguments       = %s\na" % (self.get_job_dir(processing_id))
        jdl += "GetEnv          = False\n"
        jdl += "Output          = " + 'processing_%s' % long_id + ".$(ClusterId).$(ProcId).out\n"
        jdl += "Error           = " + 'processing_%s' % long_id + ".$(ClusterId).$(ProcId).err\n"
        jdl += "Log             = " + 'processing_%s' % long_id + ".$(ClusterId).$(ProcId).log\n"
        jdl += "stream_output   = False\n"
        jdl += "stream_error    = False\n"
        # jdl += 'Requirements = ((Arch == "X86_64") && (regexp("SLC",OpSysLongName)))\n'
        # jdl += 'Requirements = ((Arch == "X86_64") && (regexp("CentOS",OpSysLongName)))\n'
        # jdl += "transfer_input_files = file1, file2\n"
        jdl += "should_transfer_files = yes\n"

        tf_inputs = [script_name]
        if input_files:
            tf_inputs = tf_inputs + input_files
        tf_outputs = output_files

        # self.logger.info("tf_inputs: %s, tf_outputs: %s" % (str(tf_inputs), str(tf_outputs)))

        # if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
        #     proxy_filename = os.path.basename(os.environ['X509_USER_PROXY'])
        #     tf_inputs = tf_inputs + [os.environ['X509_USER_PROXY']]

        if tf_inputs:
            jdl += "transfer_input_files = %s\n" % (str(','.join(tf_inputs)))
        if tf_outputs:
            jdl += "transfer_output_files = %s\n" % (str(','.join(tf_outputs)))

        jdl += "WhenToTransferOutput = ON_EXIT_OR_EVICT\n"
        jdl += "OnExitRemove         = TRUE\n"
        # jdl += '+JobFlavour = "espresso"\n'
        # jdl += '+JobFlavour = "tomorrow"\n'
        # jdl += '+JobFlavour = "testmatch"\n'
        # jdl += '+JobFlavour = "nextweek"\n'
        jdl += '+JobType="ActiveLearning"\n'
        # jdl += '+AccountingGroup ="group_u_ATLASWISC.all"\n'
        jdl += '+Processing_id = "%s"\n' % long_id
        jdl += "RequestCpus = 1\n"
        if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
            jdl += "x509userproxy = %s\n" % str(os.environ['X509_USER_PROXY'])
        jdl += "Queue 1\n"

        submit_file = 'processing_%s.jdl' % long_id
        submit_file = os.path.join(self.get_working_dir(processing), submit_file)
        with open(submit_file, 'w') as f:
            f.write(jdl)
        return submit_file, None

    def get_input_files(self, processing):
        return []

    def get_output_files(self, processing):
        return []

    def submit_condor_processing(self, processing):
        jdl_file, err_msg = self.generate_processing_submit_file(processing)
        if not jdl_file:
            return None, err_msg

        cmd = "condor_submit " + jdl_file
        status, output, error = run_command(cmd)
        jobid = None
        self.logger.info("submiting the job to cluster: %s" % cmd)
        self.logger.info("status: %s, output: %s, error: %s " % (status, output, error))
        if status == 0 or str(status) == '0':
            if output and 'submitted to cluster' in output:
                for line in output.split('\n'):
                    if 'submitted to cluster' in line:
                        jobid = line.split(' ')[-1].replace('.', '')
                        return jobid, None
        return None, output + error

    def get_job_err_message(self, job_workdir, job_err):
        try:
            if not job_err:
                return ''
            if not job_err.startswith("/") and job_workdir:
                job_err = os.path.join(job_workdir, job_err)
            if not os.path.exists(job_err):
                return ''
            with open(job_err, "r") as myfile:
                data = myfile.readlines()
            data = str(data)
            data = data[-1000:]
            return data
        except Exception as e:
            self.logger.error("Failed to read job error file(workdir: %s, error file: %s): %s" % (job_workdir, job_err, e))
        return ''

    def poll_condor_job_status(self, processing, job_id):
        # 0 Unexpanded     U
        # 1 Idle           I
        # 2 Running        R
        # 3 Removed        X
        # 4 Completed      C
        # 5 Held           H
        # 6 Submission_err E
        cmd = "condor_q -format '%s' ClusterId  -format ' %s' Processing_id -format ' %s' JobStatus -format ' %s' Iwd -format ' %s' Cmd -format ' %s' Err " + str(job_id)
        status, output, error = run_command(cmd)
        self.logger.info("poll job status: %s" % cmd)
        self.logger.info("status: %s, output: %s, error: %s" % (status, output, error))
        if status == 0 and len(output) == 0:
            cmd = "condor_history -format '%s' ClusterId  -format ' %s' Processing_id -format ' %s' JobStatus -format ' %s' Iwd -format ' %s' Cmd -format ' %s' Err " + str(job_id)
            status, output, error = run_command(cmd)
            self.logger.info("poll job status: %s" % cmd)
            self.logger.info("status: %s, output: %s, error: %s" % (status, output, error))

        ret_err = ''
        job_cmd_msg, job_err_msg = '', ''
        if status == 0:
            lines = output.split('\n')
            for line in lines:
                c_job_id, c_processing_id, c_job_status, job_workdir, job_cmd, job_err = line.split(' ')
                if str(c_job_id) != str(job_id):
                    continue

                processing_id = self.get_long_id(processing)
                c_job_status = int(c_job_status)
                if c_processing_id != processing_id:
                    final_job_status = ProcessingStatus.Failed
                    ret_err = 'jobid and the processing_id mismatched'
                else:
                    job_status = c_job_status
                    if job_status < 2:
                        final_job_status = ProcessingStatus.Submitted
                    elif job_status == 2:
                        final_job_status = ProcessingStatus.Submitted
                    elif job_status == 2:
                        final_job_status = ProcessingStatus.Running
                    elif job_status == 3:
                        final_job_status = ProcessingStatus.Cancelled
                    elif job_status == 4:
                        final_job_status = ProcessingStatus.Finished
                    else:
                        final_job_status = ProcessingStatus.Failed

                    if final_job_status in [ProcessingStatus.Failed]:
                        job_cmd_msg = self.get_job_err_message(job_workdir, job_cmd)
                        job_cmd_msg = job_cmd_msg[-500:]
                        job_err_msg = self.get_job_err_message(job_workdir, job_err)
        else:
            final_job_status = ProcessingStatus.Submitted

        # if output:
        #     ret_err += output
        if error:
            ret_err += error
        if job_cmd_msg:
            ret_err += "Command output: " + job_cmd_msg
        if job_err_msg:
            ret_err += "Stderr: " + job_err_msg

        return final_job_status, ret_err
