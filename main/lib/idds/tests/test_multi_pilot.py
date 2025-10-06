"""
Resource Wrapper (resource_wrapper.py)

A simple open-source Python resource manager that:
- Reserves an initial pool of cores and memory (logical reservation).
- Lets you submit jobs at runtime that request cores and memory.
- Uses `taskset` to pin job processes to specific CPU cores (unless running inside Slurm).
- Can optionally launch jobs using `srun` if inside a Slurm allocation.
- Attempts to enforce memory limits via `systemd-run --scope -p MemoryLimit=...` when available (outside Slurm).
- Supports a tiny plugin mechanism: plugins live in ./plugins and expose an ``inject(manager)`` function.

Notes:
- Designed for Linux. Requires `taskset` (util-linux). Memory enforcement requires `systemd-run` (systemd).
- Inside Slurm: taskset and systemd-run are skipped; instead jobs are wrapped with `srun`.
- Run with Python 3.8+.

Example usage:
    python3 resource_wrapper.py --cores 50 --mem 100

Inside the interactive prompt:
    submit sleep 60 --cores 4 --mem 2        # submit `sleep 60` requesting 4 cores and 2GB
    list                                     # list jobs
    load_plugin plugins/example_plugin.py   # load a plugin file
    quit

"""

import abc
import argparse
import logging
import os
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

# ---------- Utilities ----------

def which(prog: str) -> Optional[str]:
    return shutil.which(prog)


def setup_logging(log_level=None):
    if type(loglevel) in [str]:
        loglevel = loglevel.upper()
        loglevel = getattr(logging, loglevel)

    logging.basicConfig(stream=sys.stdout, level=loglevel, format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')


# ---------- PanDA communicator ----------

class PanDACommunicator(metaclass=abc.ABCMeta):
    def __init__(self, auth_type="token", base_url=None, timeout=120):
        self.base_url = base_url
        self.auth_type = auth_type
        self.timeout = timeout

        self.panda_jsid = os.environ.get("PANDA_JSID", None)
        self.harvester_id = os.environ.get("HARVESTER_ID", None)
        self.harvester_worker_id = os.environ.get("HARVESTER_WORKER_ID", None)

        self.panda_auth_dir = os.environ.get("PANDA_AUTH_DIR", None)
        self.panda_auth_token = os.environ.get("PANDA_AUTH_TOKEN", None)
        self.panda_auth_orgin = os.environ.get("PANDA_AUTH_ORIGIN", None)

        self.session = requests.session()

        self.logger = logging.getLogger(self.get_class_name())

    def get_class_name(self):
        return self.__class__.__name__

    def renew_token(self):
        # todo
        pass

    def get_request_response(self, command, type='GET', data=None, headers=None):
        try:
            result = None
            if not headers:
                headers={"Accept": "application/json", "Connection": "close"}

            if self.auth_type == 'token':
                headers["Authorization"] = f"Bearer {self.panda_auth_token}"
                headers["Origin"] = self.panda_auth_orgin
            else:
                # x509
                pass

            url = os.path.join(self.base_url, 'server/panda', command)

            if type == 'GET':
                result = self.session.get(url, timeout=self.timeout, headers=headers, verify=False)
            elif type == 'PUT':
                result = self.session.put(url, data=json_dumps(data), timeout=self.timeout, headers=headers, verify=False)
            elif type == 'POST':
                result = self.session.post(url, data=json_dumps(data), timeout=self.timeout, headers=headers, verify=False)
            elif type == 'DEL':
                result = self.session.delete(url, data=json_dumps(data), timeout=self.timeout, headers=headers, verify=False)
            else:
                return

            if result.status_code = 200:
                return True, result
            else:
                err_msg = f"status code: {result.status_code}, text: {result.text}"
        except Exception as ex:
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"failed to put with {err_type}:{err_value} "
            err_msg += traceback.format_exc()
        return False, err_msg

    def get_job_statistics(self):
        tmp_stat, tmp_res = self.get_request_response("getJobStatisticsPerSite", {})
        stats = {}
        if tmp_stat is False:
            ret_msg = "FAILED"
            core_utils.dump_error_message(tmp_log, tmp_res)
        else:
            try:
                tmp_stats = pickle.loads(tmp_res.content)
                for site in tmp_stats:
                    if site.startswith(self.site) and 'Merge' not in site and 'Multi' not in site:   # exclude Multi/Merge queues
                        stats[site] = tmp_stats[site]
                ret_msg = "OK"
            except Exception:
                ret_msg = "Exception"
                core_utils.dump_error_message(tmp_log)

        return stats, ret_msg

    def get_jobs(self,site_name, node_name, computing_element, n_jobs, additional_criteria={}):
        # get logger
        selflogger.debug(f"try to get {n_jobs} jobs")
        data = {}
        data["siteName"] = site_name
        data["node"] = node_name
        data["prodSourceLabel"] = prod_source_label
        data["computingElement"] = computing_element
        data["nJobs"] = n_jobs
        data["schedulerID"] = self.harvestr_id
        if additional_criteria:
            for tmpKey, tmpVal in additional_criteria.items():
                data[tmpKey] = tmpVal
        
        tmpStat, tmpRes = self.get_request_response("getJob", data)
        tmpLog.debug(f"getJob for {n_jobs} jobs {sw.get_elapsed_time()}")
        errStr = "OK"
        if tmpStat is False:
            errStr = 
        else:
            try:
                tmpDict = tmpRes.json()
                tmpLog.debug(f"StatusCode={tmpDict['StatusCode']}")
                if tmpDict["StatusCode"] == 0:
                    tmpLog.debug(f"got {len(tmpDict['jobs'])} jobs")
                    return tmpDict["jobs"], errStr
                else:
                    if "errorDialog" in tmpDict:
                        errStr = tmpDict["errorDialog"]
                    else:
                        errStr = f"StatusCode={tmpDict['StatusCode']}"
                return [], errStr
            except Exception:
                errStr = 
        return [], errStr

    def update_jobs(self, jobs_list=[], events_list=[]):
        # update events
        for event_ranges in events_lsit:
            tmpLogG.debug(f"update {len(eventSpecs)} events for PandaID={jobSpec.PandaID}")
            tmpRet = self.update_event_ranges(event_ranges)
            if tmpRet["StatusCode"] == 0:
                for eventSpec, retVal in zip(eventSpecs, tmpRet["Returns"]):
                    if retVal in [True, False] and eventSpec.is_final_status():
                        eventSpec.subStatus = "done"

        # update jobs in bulk
        nLookup = 100
        iLookup = 0
        chunks = jobs_list.split(0, len(jobs_list), nLookup)
        for chunk in chunks:
            harvester_id = self.harvester_id
            tmpData = {"jobList": json.dumps(chunk), "harvester_id": harvester_id}
            tmpStat, tmpRes = self.get_request_response("updateJobsInBulk", tmpData)
            retMaps = None
            errStr = ""
            if tmpStat is False:
                errStr = 
            else:
                try:
                    tmpStat, retMaps = tmpRes.json()
                    if tmpStat is False:
                        tmpLogG.error(f"updateJobsInBulk failed with {retMaps}")
                        retMaps = None
                except Exception:
                    errStr = core_utils.dump_error_message(tmpLogG)
            if retMaps is None:
                retMap = {}
                retMap["content"] = {}
                retMap["content"]["StatusCode"] = 999
                retMap["content"]["ErrorDiag"] = errStr
                retMaps = [json.dumps(retMap)] * len(jobSpecSubList)
            for jobSpec, retMap, data in zip(jobSpecSubList, retMaps, dataList):
                tmpLog = self.make_logger(f"id={id} PandaID={jobSpec.PandaID}", method_name="update_jobs")
                try:
                    retMap = json.loads(retMap["content"])
                except Exception:
                    errStr = f"failed to json_load {str(retMap)}"
                    retMap = {}
                    retMap["StatusCode"] = 999
                    retMap["ErrorDiag"] = errStr
                tmpLog.debug(f"data={str(data)}")
                tmpLog.debug(f"done with {str(retMap)}")
                retList.append(retMap)
            iLookup += nLookup
        tmpLogG.debug("done" + sw.get_elapsed_time())
        return retList

    # get events
    def get_event_ranges(self, data_map, scattered, base_path):
        retStat = False
        retVal = dict()
        try:
            getEventsChunkSize = harvester_config.pandacon.getEventsChunkSize
        except Exception:
            getEventsChunkSize = 5120
        for pandaID, data in data_map.items():
            # get logger
            tmpLog = self.make_logger(f"PandaID={data['pandaID']}", method_name="get_event_ranges")
            if "nRanges" in data:
                nRanges = data["nRanges"]
            else:
                nRanges = 1
            if scattered:
                data["scattered"] = True
            if "isHPO" in data:
                isHPO = data["isHPO"]
                del data["isHPO"]
            else:
                isHPO = False
            if "sourceURL" in data:
                sourceURL = data["sourceURL"]
                del data["sourceURL"]
            else:
                sourceURL = None
            tmpLog.debug(f"start nRanges={nRanges}")
            while nRanges > 0:
                # use a small chunk size to avoid timeout
                chunkSize = min(getEventsChunkSize, nRanges)
                data["nRanges"] = chunkSize
                tmpStat, tmpRes = self.post_ssl("getEventRanges", data)
                if tmpStat is False:
                    core_utils.dump_error_message(tmpLog, tmpRes)
                else:
                    try:
                        tmpDict = tmpRes.json()
                        if tmpDict["StatusCode"] == 0:
                            retStat = True
                            retVal.setdefault(data["pandaID"], [])
                            if not isHPO:
                                retVal[data["pandaID"]] += tmpDict["eventRanges"]
                            else:
                                for event in tmpDict["eventRanges"]:
                                    event_id = event["eventRangeID"]
                                    task_id = event_id.split("-")[0]
                                    point_id = event_id.split("-")[3]
                                    # get HP point
                                    tmpSI, tmpOI = idds_utils.get_hp_point(harvester_config.pandacon.iddsURL, task_id, point_id, tmpLog, self.verbose)
                                    if tmpSI:
                                        event["hp_point"] = tmpOI
                                        # get checkpoint
                                        if sourceURL:
                                            tmpSO, tmpOO = self.download_checkpoint(sourceURL, task_id, data["pandaID"], point_id, base_path)
                                            if tmpSO:
                                                event["checkpoint"] = tmpOO
                                        retVal[data["pandaID"]].append(event)
                                    else:
                                        core_utils.dump_error_message(tmpLog, tmpOI)
                            # got empty
                            if len(tmpDict["eventRanges"]) == 0:
                                break
                    except Exception:
                        core_utils.dump_error_message(tmpLog)
                        break
                nRanges -= chunkSize
            tmpLog.debug(f"done with {str(retVal)}")
        return retStat, retVal

    # update events
    def update_event_ranges(self, event_ranges):
        self.logger.debug(f"data={str(data)}")
        tmpStat, tmpRes = self.get_request_response("updateEventRanges", event_ranges)
        retMap = None
        if tmpStat is False:
            core_utils.dump_error_message(tmp_log, tmpRes)
        else:
            try:
                retMap = tmpRes.json()
            except Exception:
                core_utils.dump_error_message(tmp_log)
        if retMap is None:
            retMap = {}
            retMap["StatusCode"] = 999
        tmp_log.debug(f"done updateEventRanges with {str(retMap)}")
        return retMap

    # get commands
    def get_commands(self, n_commands):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = self.make_logger(f"harvesterID={harvester_id}", method_name="get_commands")
        tmpLog.debug(f"Start retrieving {n_commands} commands")
        data = {}
        data["harvester_id"] = harvester_id
        data["n_commands"] = n_commands
        tmp_stat, tmp_res = self.post_ssl("getCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmpLog.debug(f"Commands {tmp_dict['Commands']}")
                    return tmp_dict["Commands"]
                return []
            except Exception:
                core_utils.dump_error_message(tmpLog, tmp_res)
        return []

    # send ACKs
    def ack_commands(self, command_ids):
        harvester_id = harvester_config.master.harvester_id
        tmpLog = self.make_logger(f"harvesterID={harvester_id}", method_name="ack_commands")
        tmpLog.debug(f"Start acknowledging {len(command_ids)} commands (command_ids={command_ids})")
        data = {}
        data["command_ids"] = json.dumps(command_ids)
        tmp_stat, tmp_res = self.post_ssl("ackCommands", data)
        if tmp_stat is False:
            core_utils.dump_error_message(tmpLog, tmp_res)
        else:
            try:
                tmp_dict = tmp_res.json()
                if tmp_dict["StatusCode"] == 0:
                    tmpLog.debug("Finished acknowledging commands")
                    return True
                return False
            except Exception:
                core_utils.dump_error_message(tmpLog, tmp_res)
        return False


# ---------- Job Dataclass ----------
@dataclass
class Job:
    id: str
    cmd: str
    cores: List[int]
    mem_gb: float
    proc: Optional[subprocess.Popen] = None
    status: str = "PENDING"
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    job_data = None

    def __init__(self, work_dir, job_data):
        self.job_data = job_data
        self.cores = job_data.get("coreCount", 1)
        self.id = job_data.get("PandaID")
        self.mem_gb = job_data.get("minRamCount") / 1000.0
        self.task_id = job_data.get('taskID')
        self.work_dir = os.path.join(work_dir, str(self.id))
        self.job_status = None
        self.event_status = None
        self.final_job_status = None

    def get_job_cmd(self):
        # put the file in 'PILOT_HOME' for push mode. Pilot will get the job from this file to run
        # https://github.com/PanDAWMS/pilot3/blob/master/pilot/control/job.py#L1792
        # If something wrong, try 'HARVESTER_WORKDIR' and 'HPCJobs.json'
        panda_job_data = os.path.join(self.work_dir, 'pandaJobData.out')
        with open(panda_job_data, "w") as f:
            json.dump(self.job_data, f)

        cmd = f"""
#!/bin/bash

# example
# echo ${rubin_wrapper} ${piloturl} -s SLAC_Rubin_8G -r SLAC_Rubin_8G -q SLAC_Rubin_8G -i PR -w generic --allow-same-user false --pilot-user rubin --es-executor-type fineGrainedProc --noproxyverification --url https://usdf-panda-server.slac.stanford.edu:8443 --harvester-submit-mode PULL --queuedata-url https://usdf-panda-server.slac.stanford.edu:8443/cache/schedconfig/SLAC_Rubin_8G.all.json --storagedata-url /sdf/home/l/lsstsvc1/cric/cric_ddmendpoints.json --use-realtime-logging --realtime-logname Panda-RubinLog --pilotversion 3  --pythonversion 3 --localpy  | sed -e "s/^/pilot_\${SLURM_PROCID}: /"

echo ${rubin_wrapper} ${piloturl} -s {self.site} -r {self.site} -q {self.site} -i PR -w generic --allow-same-user false --pilot-user rubin --es-executor-type fineGrainedProc --noproxyverification --url {self.base_url} --noserverupdate --harvester-submit-mode PUSH --queuedata-url {self.base_url}/cache/schedconfig/{self.site}.all.json --storagedata-url /sdf/home/l/lsstsvc1/cric/cric_ddmendpoints.json --use-realtime-logging --realtime-logname Panda-RubinLog --pilotversion 3  --pythonversion 3 --localpy  | sed -e "s/^/pilot_\${SLURM_PROCID}: /"

# set pilot home.
export PILOT_HOME={self.work_dir}

${rubin_wrapper}  ${piloturl} -s {self.site} -r {self.site} -q {self.site} -i PR -w generic --allow-same-user false --pilot-user rubin --es-executor-type fineGrainedProc --noproxyverification --url {self.base_url} --noserverupdate --harvester-submit-mode PUSH --queuedata-url {self.base_url}/cache/schedconfig/{self.site}.all.json --storagedata-url /sdf/home/l/lsstsvc1/cric/cric_ddmendpoints.json --use-realtime-logging --realtime-logname Panda-RubinLog --pilotversion 3  --pythonversion 3 --localpy  | sed -e "s/^/pilot_\${SLURM_PROCID}: /"
        """
        cmd_script = oe.path.join(self.work_dir, 'my_panda_run_script')
        with open(cmd_script, 'w') as f:
            f.write(cmd)
        os.chmod("a+rx")
        return cmd_script

    def get_status_report(self):
        # pilot writes the updates to a file in PILOT_HOME. This one will get this file and then update in bulk
        # --noserverupdate: pilot will not update panda server.
        # harvester mode
        #     elif ('HARVESTER_ID' in os.environ or 'HARVESTER_WORKER_ID' in os.environ) and args.harvester_submitmode.lower() == 'push':
        # https://github.com/PanDAWMS/pilot3/blob/master/pilot/util/harvester.py#L58
        job_status_file = os.path.join(self.work_dir, 'worker_attributes.json')
        if os.path.exist(job_status_file):
            self.job_status = json.load(job_status_file)
            move(job_status_file, 'back.'+job_status_file+f".{datetime.datetime.ntcnow()}")

        event_status = os.path.join(self.work_dir, 'event_status.dump.json')
        all_files = list_files_startswith(event_status)
        for file in all_files:
            self.event_status = json.load(file)
            move(event_status, 'back.'+event_status+f".{datetime.datetime.ntcnow()}")

        final_job_status_file = os.path.join(self.work_dir, 'jobReport.json')
        if os.path.exists(final_job_status_file):
            self.final_job_status = json.load(final_job_status_file)
            mv(final_job_status_file, 'back.'+final_job_status_file+f".{datetime.datetime.ntcnow()}"


# ---------- Resource Manager ----------
class ResourceManager:
    def __init__(self, active_hours: float =12, reserve_cores: int = 50, reserve_mem_gb: float = 100.0):
        # Detect system cores; if fewer than requested, create virtual numbered cores
        self.system_cpu_count = os.cpu_count() or 1
        self.total_core_space = max(self.system_cpu_count, reserve_cores)
        self.lock = threading.Lock()

        # Represent cores as integers 0..total_core_space-1
        self.free_cores: Set[int] = set(range(self.total_core_space))
        self.reserved_cores: Set[int] = set()  # currently allocated to our jobs

        # Logical memory pool (GB)
        self.total_mem_gb = reserve_mem_gb
        self.free_mem_gb = reserve_mem_gb

        self.jobs: Dict[str, Job] = {}

        # Detect helpers
        self.has_taskset = which('taskset') is not None
        self.has_systemd_run = which('systemd-run') is not None

        # Detect if inside Slurm
        self.in_slurm = 'SLURM_JOB_ID' in os.environ
        if self.in_slurm:
            print(f"Detected Slurm environment: SLURM_JOB_ID={os.environ.get('SLURM_JOB_ID')}")
            # Avoid conflicting with Slurm's resource binding
            self.has_taskset = False
            self.has_systemd_run = False

        print(f"ResourceManager started: total_core_space={self.total_core_space}, "
              f"reserved_mem_gb={self.total_mem_gb}")
        print(f"taskset available: {self.has_taskset}, systemd-run available: {self.has_systemd_run}, in_slurm={self.in_slurm}")

        self.start_time = daytime.ntc_now()
        self.expect_end_time = None
        self.task_wall_time = {}
        self.node_name = sef.get_short_hostname()

        self.panda_communicator = PanDACommunicator(site=self.site, work_dir=self.work_dir, base_url=self.panda_server)

    def _claim_cores(self, n: int) -> Optional[List[int]]:
        with self.lock:
            if n <= 0:
                return []
            if len(self.free_cores) < n:
                return None
            chosen = sorted(list(self.free_cores))[:n]
            for c in chosen:
                self.free_cores.remove(c)
                self.reserved_cores.add(c)
            return chosen

    def _release_cores(self, cores: List[int]):
        with self.lock:
            for c in cores:
                if c in self.reserved_cores:
                    self.reserved_cores.remove(c)
                    self.free_cores.add(c)

    def _claim_mem(self, mem_gb: float) -> bool:
        with self.lock:
            if mem_gb <= 0:
                return True
            if self.free_mem_gb + 1e-9 < mem_gb:
                return False
            self.free_mem_gb -= mem_gb
            return True

    def _release_mem(self, mem_gb: float):
        with self.lock:
            self.free_mem_gb += mem_gb

    def terminate_job(self, job):
        with self.lock:
            self.terminating_jobs[job.job_id] = job
            del self.job[job.job_id]

    def terminated_job(self, job):
        with self.lock:
            self.terminated_jobs[job.job_id] = job
            del self.terminating_job[job.job_id]

    def submit_job(self, job: Job) -> Optional[str]:
        """Attempt to reserve resources and start the job. Returns job id or None on failure."""
        claimed_cores = self._claim_cores(job.cores)
        if claimed_cores is None:
            print(f"Not enough free cores to allocate {cores} cores")
            return None
        if not self._claim_mem(job.mem_gb):
            print(f"Not enough free memory to allocate {mem_gb} GB")
            # rollback cores
            self._release_cores(claimed_cores)
            return None

        # job_id = str(uuid.uuid4())
        self.jobs[job_id] = job
        job.expected_wall_time = self.task_wall_time.get(job.task_id, None)


        # Launch in a background thread so the call is non-blocking
        t = threading.Thread(target=self._start_job_process, args=(job,), daemon=True)
        t.start()
        return job_id

    def _start_job_process(self, job: Job):
        job.status = 'STARTING'
        core_list_str = ','.join(str(c) for c in job.cores)

        # Build base command
        inner_cmd = job.cmd

        if self.in_slurm:
            # Use srun inside Slurm, rely on Slurm for CPU/memory binding
            wrapper_cmd = ['srun', '--export=ALL', '--cpu-bind=none',  f'--cpus-per-task={len(job.cores)}']
            if job.mem_gb > 0:
                wrapper_cmd.append(f'--mem={int(job.mem_gb)}G')
            wrapper_cmd += ['bash', '-c', inner_cmd]
        else:
            # If taskset is available, use it to pin CPUs
            if self.has_taskset:
                wrapper_cmd = ['taskset', '-c', core_list_str] + wrapper_cmd

        try:
            job.started_at = time.time()
            job.status = 'RUNNING'
            proc = subprocess.Popen(wrapper_cmd)
            job.proc = proc
            print(f"Job {job.id} started: pid={proc.pid}, cores={job.cores}, mem_gb={job.mem_gb}")
            proc.wait()
            job.finished_at = time.time()
            job.status = 'FINISHED' if proc.returncode == 0 else f'FAILED({proc.returncode})'
            print(f"Job {job.id} finished with returncode={proc.returncode}")

            if job.task_id not in self.task_wall_time or self.task_wall_time[job.task_id] < job.finished_at - job.started_at:
                self.task_wall_time[job.task_id] = job.finished_at - job.started_at
        except Exception as e:
            job.status = f'ERROR: {e}'
            job.finished_at = time.time()
            print(f"Job {job.id} error: {e}")
        finally:
            self._release_cores(job.cores)
            self._release_mem(job.mem_gb)
            self.terminate_job(job)

    def list_jobs(self) -> List[Job]:
        return list(self.jobs.values())

    def get_status_snapshot(self) -> Dict:
        with self.lock:
            return {
                'total_core_space': self.total_core_space,
                'free_cores': sorted(self.free_cores),
                'reserved_cores': sorted(self.reserved_cores),
                'total_mem_gb': self.total_mem_gb,
                'free_mem_gb': self.free_mem_gb,
                'num_running_jobs': len(self.jobs),
                'terminated_jobs': len(self.terminated_jobs)
            }

    def get_memory_per_job_from_site(self, site):
        return site.split("_")[-1].replace("G")

    def get_expect_left_time(self):
        expect_left_time = None
        for job_id in self.jobs:
            if self.jobs[job_id].expected_end_at and self.jobs[job_id].expected_end_at > expect_left_time:
                expect_left_time = self.jobs[job_id].expected_end_at
        seconds = expect_left_time - datetime.datetime.ntc_now()
        return seconds

    def schedule(self):
        site_statistics = self.panda_communicator.get_job_statistics()
        total_activated_jobs = 0
        for site in site_statistics:
            total_activated_jobs += site_statistics[site]['activated']
        for site in site_statistics:
            site_statistics[site]['quote'] = site_statistics[site]['activated'] * 1.0 / total_activated_jobs
            site_statistics[site]['quote_cores'] = site_statistics[site]['quote'] * self.free_cores
            site_statistics[site]['quote_memory'] = site_statistics[site]['quote'] * self.free_mem_gb

        if total_activated_jobs < 1:
            return
        # expected left time for current running jobs
        expect_left_time = self.get_expect_left_time()
        if self.free_cores > self.reserved_cores * 0.5 and expect_left_time < 30 * 60:
            # core usage < 0.5 and expect_left_time < 30 minutes, not scheduling new jobs
            return
        if self.start_at + self.active_hours > datetime.datetime.ntc_now():
            # has been running for {self.active_hours} hours
            if expect_left_time < 30 * 60:
                return
            else:
                left_time_to_get_jobs = expect_left_time

        # todo: order sites by 32G, 28G, .... (big memory jobs first)
        for site in site_statistics:
            memory_per_job = self.get_memory_per_job_from_site(site)
            num_jobs = int(site_statistics[site]['quote_memory'] / memory_per_job)
            if num_jobs:
                # left_time_to_get_jobs
                jobs = self.panda_communicator.get_jobs(site_name=site, node_name=self.node_name, computing_element=site, n_jobs=num_jobs, walltime=left_time_to_get_jobs, additional_criteria)
                for job in jobs:
                    new_job = Job(job_data)
                    self.submit_job(new_job)

    def report(self):
        for job_id in self.terminating_jobs:
            job = self.terminating_jobs[job_id]
            job.get_status_report()
            if job.status_report:
                self.status_reports.append(job.status_report)
                job.status_report = None
            if job.event_status:
                self.event_reports.append(job.event_status)
                job.event_status = None
            if job.final_status_report:
                self.final_status_reports.append(job.final_status_report)
                job.final_status_report = None
            self.terminated_job(job)
        # update info to PanDA in bulk
        self.panda_communicator.update_jobs(self.status_reports)
        self.status_reports = []
        self.panda_communicator.update_events(self.event_reports)
        self.event_reports = []
        self.panda_communicator.update_jobs(self.final_status_reports)
        self.final_status_reports = []

    def handle_commands(self):
        # todo
        # the commands will kill jobs
        pass

    def run(self):
        while True:
            self.schedule()
            self.report()
            self.handle_commands()
            time.sleep(10)


# ---------- Main ----------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple resource wrapper')
    parser.add_argument('--site', type=str, default=None, help='Site to run jobs')
    parser.add_argument('--cores', type=int, default=10, help='Initial cores to reserve (logical)')
    parser.add_argument('--mem', type=float, default=40.0, help='Initial memory to reserve (GB)')
    parser.add_argument('--work_dir', type=str, default=None, help='Initial work directory')
    parser.add_argument('--panda_server', type=str, default=None, help='PanDA server')
    parser.add_argument('--active_hours', type=float, default=12.0, help='Active hours to get jobs')
    args = parser.parse_args()

    manager = ResourceManager(site=args.site, reserve_cores=args.cores, reserve_mem_gb=args.mem, work_dir=args.work_dir, panda_server=args.panda_server)
    manager.run()
