import uuid
import subprocess
import shlex
import os
import glob


def runtasklocal(taskid, task_parameters):
    total_n_jobs = task_parameters['total_n_jobs']
    os.mkdir(taskid)
    print('evgen')
    for job in range(total_n_jobs):
        job_parameters = {
            'p1': task_parameters['p1'],
            'p2': task_parameters['p2'],
            'nevents': int(task_parameters['nevents'] / total_n_jobs),
            'outputfile': '{}/out{}.json'.format(taskid, str(job).zfill(5))
        }
        cmdline = 'python simplescript.py {p1} {p2} {nevents} {outputfile}'.format(
            **job_parameters
        )
        print('subjob {}'.format(job), job_parameters)
        print('> {}'.format(cmdline))

        subprocess.check_call(shlex.split(cmdline))

    merge_job_parameters = {
        'outputfile': '{}/merged.json'.format(taskid),
        'nevents': task_parameters['nevents'],
        'filelist': ' '.join(glob.glob('{}/out*.json'.format(taskid)))
    }
    cmdline = 'python merge.py {outputfile} {nevents} {filelist}'.format(**merge_job_parameters)
    print('merge', merge_job_parameters)
    print('> {}'.format(cmdline))
    subprocess.check_call(shlex.split(cmdline))


if __name__ == '__main__':

    task_parameters = {'p1': 0.5, 'p2': 0.5, 'nevents': 1000, 'total_n_jobs': 5}

    taskid = 'task-' + str(uuid.uuid4()).split('-')[0]
    runtasklocal(taskid, task_parameters)
