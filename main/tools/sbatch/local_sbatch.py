#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


import os
import re
import sys
import subprocess


def get_my_username():
    username = os.getlogin()
    return username


def get_queued_jobs(username, partition, debug=False):
    status = False
    num_pending_jobs = 0
    # command = f"squeue -u {username} --partition={partition} | grep -e PD -e CF"
    command = f"squeue -u {username} --partition={partition}"
    if debug:
        print(f"command: {command.split()}")
    p = subprocess.Popen(command.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    ret_code = p.returncode

    # if debug:
    #     print(f"returncode: {ret_code}, stdout: {stdout}, stderr: {stderr}")
    if ret_code == 0:
        stdout_str = stdout if (isinstance(stdout, str) or stdout is None) else stdout.decode()
        # stderr_str = stderr if (isinstance(stderr, str) or stderr is None) else stderr.decode()
        # if debug:
        #     print(f"stout: {stdout_str}, stderr: {stderr_str}")

        num_pending_jobs = 0
        for line in stdout_str.split("\n"):
            if len(line) == 0 or line.startswith("JobID") or line.startswith("--"):
                continue

            batch_status = line.split()[4].strip()
            if batch_status in ["CF", "PD"]:
                num_pending_jobs += 1
        if debug:
            print(f"number of pending jobs in partition {partition} with user {username}: {num_pending_jobs}")
        status = True
    else:
        if debug:
            print(f"returncode: {ret_code}, stdout: {stdout}, stderr: {stderr}")

    return status, num_pending_jobs


def create_new_submit_file(old_submit_file, new_submit_file, new_partition):
    with open(old_submit_file, 'r') as f:
        content = f.read()

    # Replace the pattern --partition=***
    updated_content = re.sub(r'--partition=\S+', f'--partition={new_partition}', content)

    with open(new_submit_file, 'w') as f:
        f.write(updated_content)


if __name__ == "__main__":
    partitions = ['milano', 'roma']
    all_args = sys.argv
    parameters = sys.argv[1:]

    debug = False

    try:
        username = get_my_username()
        num_pending_by_partition = {}
        for p in partitions:
            status, num_jobs = get_queued_jobs(username, p, debug=debug)
            if status:
                num_pending_by_partition[p] = num_jobs

        if debug:
            print(f"num_pending_by_partition: {num_pending_by_partition}")

        sorted_num_pending = dict(sorted(num_pending_by_partition.items(), key=lambda item: item[1]))
        selected_partition = None
        if sorted_num_pending:
            selected_partition = list(sorted_num_pending.keys())[0]
        if debug:
            print(f"selected_partition: {selected_partition}")

        if selected_partition:
            submit_file = parameters[-1]
            new_submit_file = submit_file.strip() + ".new_local_sbatch.sh"
            create_new_submit_file(submit_file, new_submit_file, selected_partition)
            if debug:
                print(f"new_submit_file: {new_submit_file}")
            parameters[-1] = new_submit_file
    except Exception as ex:
        if debug:
            print(f"Exception: {ex}")

    new_command = ['sbatch'] + parameters
    if debug:
        print(f"New command: {new_command}")

    result = subprocess.run(new_command)
    sys.exit(result.returncode)
