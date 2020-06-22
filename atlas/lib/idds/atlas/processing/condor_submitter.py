#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020


"""
Class of activelearning condor plubin
"""
import os


from idds.common import exceptions
from idds.common.utils import run_command
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class CondorSubmitter(ProcessingPluginBase):
    def __init__(self, workdir, **kwargs):
        super(CondorSubmitter, self).__init__(**kwargs)
        self.workdir = workdir
        self.name = 'condor'

    def __call__(self, processing, transform, input_collection, output_collection):
        raise exceptions.AgentPluginError('NotImplemented')

    def get_job_dir(self, processing_id):
        job_dir = 'processing_%s' % processing_id
        job_dir = os.path.join(self.workdir, job_dir)
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        return job_dir

    def generate_submit_script(self, processing_id, sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable=False):
        script = "#!/bin/bash\n\n"
        script += "sandbox=%s\n" % str(sandbox)
        if should_transfer_executable:
            script += "executable=%s\n" % os.path.basename(executable)
        else:
            script += "executable=%s\n" % str(executable)
        script += "arguments=%s\n" % str(arguments)
        script += "input_list=%s\n" % str(input_list)
        script += "input_json=%s\n" % str(input_json)
        script += "output_json=%s\n" % str(output_json)
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "echo 'user id:'\n"
        script += "id\n"
        script += "\n"

        if sandbox and len(sandbox) > 0:
            script += "wget $sandbox\n"
            script += 'base_sandbox="$(basename -- $sandbox)"\n'
            script += 'tar xzf $base_sandbox\n'
        if should_transfer_executable:
            script += 'chmod +x %s\n' % os.path.basename(executable)
            script += "echo '%s' '%s'\n" % (os.path.basename(executable), str(arguments))
            script += './%s %s\n' % (os.path.basename(executable), str(arguments))
        else:
            script += 'chmod +x %s\n' % str(executable)
            script += "echo '%s' '%s'\n" % (str(executable), str(arguments))
            script += '%s %s\n' % (str(executable), str(arguments))

        if 'docker' in executable:
            script += 'docker image rm -f %s\n' % str(sandbox)

        script += '\n'

        script_name = 'processing_%s.sh' % processing_id
        script_name = os.path.join(self.get_job_dir(processing_id), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        return script_name

    def generate_submit_file(self, processing_id, sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable=False):
        script_name = self.generate_submit_script(processing_id, sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable=should_transfer_executable)

        jdl = "#Agent jdl file\n"
        jdl += "Universe        = vanilla\n"
        jdl += "Notification    = Never\n"
        jdl += "initialdir      = %s\n" % self.get_job_dir(processing_id)
        jdl += "Executable      = %s\n" % script_name
        # jdl += "Arguments       = %s\na" % (self.get_job_dir(processing_id))
        jdl += "GetEnv          = False\n"
        jdl += "Output          = " + 'processing_%s' % processing_id + ".$(ClusterId).$(ProcId).out\n"
        jdl += "Error           = " + 'processing_%s' % processing_id + ".$(ClusterId).$(ProcId).err\n"
        jdl += "Log             = " + 'processing_%s' % processing_id + ".$(ClusterId).$(ProcId).log\n"
        jdl += "stream_output   = False\n"
        jdl += "stream_error    = False\n"
        # jdl += 'Requirements = ((Arch == "X86_64") && (regexp("SLC",OpSysLongName)))\n'
        # jdl += 'Requirements = ((Arch == "X86_64") && (regexp("CentOS",OpSysLongName)))\n'
        # jdl += "transfer_input_files = file1, file2\n"
        if input_json:
            jdl += "should_transfer_files = yes\n"
            if should_transfer_executable:
                jdl += "transfer_input_files = %s, %s\n" % (executable, input_json)
            else:
                jdl += "transfer_input_files = %s\n" % input_json
        else:
            if should_transfer_executable:
                jdl += "transfer_input_files = %s\n" % (executable)
        if output_json:
            jdl += "should_transfer_files = yes\n"
            jdl += "transfer_output_files = %s\n" % output_json
        jdl += "WhenToTransferOutput = ON_EXIT_OR_EVICT\n"
        jdl += "OnExitRemove         = TRUE\n"
        # jdl += '+JobFlavour = "espresso"\n'
        # jdl += '+JobFlavour = "tomorrow"\n'
        # jdl += '+JobFlavour = "testmatch"\n'
        # jdl += '+JobFlavour = "nextweek"\n'
        jdl += '+JobType="ActiveLearning"\n'
        # jdl += '+AccountingGroup ="group_u_ATLASWISC.all"\n'
        jdl += '+Processing_id = %s\n' % processing_id
        jdl += "RequestCpus = 1\n"
        if 'X509_USER_PROXY' in os.environ and os.environ['X509_USER_PROXY']:
            jdl += "x509userproxy = %s\n" % str(os.environ['X509_USER_PROXY'])
        jdl += "Queue 1\n"

        submit_file = 'processing_%s.jdl' % processing_id
        submit_file = os.path.join(self.get_job_dir(processing_id), submit_file)
        with open(submit_file, 'w') as f:
            f.write(jdl)
        return submit_file

    def submit_job(self, processing_id, sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable=False):
        jdl_file = self.generate_submit_file(processing_id, sandbox, executable, arguments, input_list, input_json, output_json, should_transfer_executable=should_transfer_executable)
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
