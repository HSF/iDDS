#!/bin/env python

import os
import sys
import subprocess
# import base64
import datetime
import tarfile


def download_extract_archive(filename):
    archive_basename = os.path.basename(filename)
    target_dir = os.getcwd()
    full_output_filename = os.path.join(target_dir, archive_basename)

    if filename.startswith("https:"):
        panda_cache_url = os.path.dirname(os.path.dirname(filename))
        os.environ["PANDACACHE_URL"] = panda_cache_url
    elif "PANDACACHE_URL" not in os.environ and "PANDA_URL_SSL" in os.environ:
        os.environ["PANDACACHE_URL"] = os.environ["PANDA_URL_SSL"]
    print("PANDACACHE_URL: %s" % os.environ.get("PANDACACHE_URL", None))

    from pandaclient import Client
    # status, output = Client.getFile(archive_basename, output_path=full_output_filename, verbose=False)
    status, output = Client.getFile(archive_basename, output_path=full_output_filename)
    print("Download archive file from pandacache status: %s, output: %s" % (status, output))
    if status != 0:
        raise RuntimeError("Failed to download archive file from pandacache")
    with tarfile.open(full_output_filename, 'r:gz') as f:
        f.extractall(target_dir)
    print("Extract %s to %s" % (full_output_filename, target_dir))
    os.remove(full_output_filename)
    print("Remove %s" % full_output_filename)


# request_id and signature are added by iDDS for build task
request_id = os.environ.get("IDDS_BUILD_REQUEST_ID", None)
signature = os.environ.get("IDDS_BUIL_SIGNATURE", None)
job_archive = sys.argv[1]
exec_str = sys.argv[2:]
exec_str = " ".join(exec_str)

if request_id is None:
    print("IDDS_BUILD_REQUEST_ID is not defined.")
    sys.exit(-1)
if signature is None:
    print("IDDS_BUIL_SIGNATURE is not defined")
    sys.exit(-1)

print("INFO: start {}".format(datetime.datetime.utcnow()))
print("INFO: job archive: {}".format(job_archive))
print("INFO: exec string: {}".format(exec_str))

current_dir = os.getcwd()

download_extract_archive(job_archive)

print("INFO: current dir: %s" % current_dir)

# add current dir to PATH
os.environ['PATH'] = current_dir + ":" + os.environ['PATH']

p = subprocess.Popen(exec_str, stdout=sys.stdout, stderr=sys.stderr,
                     shell=True, universal_newlines=True)
retcode = p.wait()
print("INFO : end {} with retcode={}".format(datetime.datetime.utcnow(), retcode))
exit(retcode)
