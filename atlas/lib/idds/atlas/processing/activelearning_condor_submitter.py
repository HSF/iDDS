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
import traceback


from idds.common import exceptions
from idds.common.constants import ProcessingStatus
from idds.common.utils import run_command
from idds.atlas.processing.base_plugin import ProcessingPluginBase
from idds.core import (catalog as core_catalog)


class ActiveLearningCondorSubmitter(ProcessingPluginBase):
    def __init__(self, workdir, **kwargs):
        super(ActiveLearningCondorSubmitter, self).__init__(**kwargs)
        self.workdir = workdir
        self.name = 'condor'

    def __call__(self, processing, transform, input_collection, output_collection):
        try:
            contents = core_catalog.get_contents_by_coll_id_status(coll_id=input_collection['coll_id'])
            files = []
            for content in contents:
                file = '%s:%s' % (content['scope'], content['name'])
                files.append(file)

            input_list = ','.join(files)
            scripts = transform['transform_metadata']['scripts']
            executable = transform['transform_metadata']['executable']
            arguments = transform['transform_metadata']['arguments']
            output_json = None
            if 'output_json' in transform['transform_metadata']:
                output_json = transform['transform_metadata']['output_json']

            job_id, outputs = self.submit_job(processing['processing_id'], scripts, executable, arguments, input_list, output_json)

            processing_metadata = processing['processing_metadata']
            processing_metadata['job_id'] = job_id
            processing_metadata['submitter'] = self.name
            if not job_id:
                processing_metadata['submit_errors'] = outputs
            else:
                processing_metadata['submit_errors'] = None

            ret = {'processing_id': processing['processing_id'],
                   'status': ProcessingStatus.Submitted,
                   'processing_metadata': processing_metadata}
            return ret
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))

    def get_job_dir(self, processing_id):
        job_dir = 'processing_%s' % processing_id
        job_dir = os.path.join(self.workdir, job_dir)
        if not os.path.exists(job_dir):
            os.makedirs(job_dir)
        return job_dir

    def generate_submit_script(self, processing_id, scripts, executable, arguments, input_list, output_json):
        script = "#!/bin/bash\n\n"
        script += "scripts=%s\n" % str(scripts)
        script += "executable=%s\n" % str(executable)
        script += "arguments=%s\n" % str(arguments)
        script += "input_list=%s\n" % str(input_list)
        script += "output_json=%s\n" % str(output_json)
        script += "\n"

        script += "env\n"
        script += "echo $X509_USER_PROXY\n"
        script += "\n"

        script += "wget $scripts\n"
        script += 'base_scripts="$(basename -- $scripts)"\n'
        script += 'tar xzf $base_scripts\n'
        script += 'chmod +x $executable\n'
        script += 'echo ./$executable $arguments\n'
        script += './$executable $arguments\n'
        script += '\n'

        script_name = 'processing_%s.sh' % processing_id
        script_name = os.path.join(self.get_job_dir(processing_id), script_name)
        with open(script_name, 'w') as f:
            f.write(script)
        return script_name

    def generate_submit_file(self, processing_id, scripts, executable, arguments, input_list, output_json):
        script_name = self.generate_submit_script(processing_id, scripts, executable, arguments, input_list, output_json)

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

    def submit_job(self, processing_id, scripts, executable, arguments, input_list, output_json):
        jdl_file = self.generate_submit_file(processing_id, scripts, executable, arguments, input_list, output_json)
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
