#!/usr/bin/python

"""
check iDDS health
"""

import json
import os
import re
import subprocess
import time


def check_command(command, check_string):
    print("Checking command : {0}".format(command))
    print("For string : {0}".format(check_string))

    tmp_array = command.split()
    output = (
        subprocess.Popen(tmp_array, stdout=subprocess.PIPE)
        .communicate()[0]
        .decode("ascii")
    )

    if re.search(check_string, output):
        print("Found the string, return 100")
        return 100
    else:
        print("String not found, return 0")
        return 0


def is_logrotate_running():
    # get the count of logrotate processes - if >=1 then logrotate is running
    output = (
        subprocess.Popen(
            "ps -eo pgid,args | grep logrotate | grep -v grep | wc -l",
            stdout=subprocess.PIPE,
            shell=True,
        )
        .communicate()[0]
        .decode("ascii")
    )

    try:
        cleaned_output = output.strip()
        n_logrotate_processes = int(cleaned_output)
    except ValueError:
        print(
            "The string has an unexpected format and couldn't be converted to an integer."
        )

    # logrotate process found
    if n_logrotate_processes >= 1:
        print("Logrotate is running")
        return True

    return False


def is_restarting():
    # get the count of logrotate processes - if >=1 then logrotate is running
    output = (
        subprocess.Popen(
            "ps -eo pgid,args | grep restart|grep http | grep -v grep | wc -l",
            stdout=subprocess.PIPE,
            shell=True,
        )
        .communicate()[0]
        .decode("ascii")
    )

    try:
        cleaned_output = output.strip()
        n_restarting_processes = int(cleaned_output)
    except ValueError:
        print(
            "The string has an unexpected format and couldn't be converted to an integer."
        )

    # logrotate process found
    if n_restarting_processes >= 1:
        print("http is restarting")
        return True

    return False


def http_availability(host):
    # check the http
    avail = 0
    if os.environ.get('X509_USER_PROXY', None):
        curl = "curl -i -k --cert $X509_USER_PROXY --key $X509_USER_PROXY --cacert $X509_USER_PROXY https://%s:8443/idds/ping" % host
        avail = check_command(curl, '"Status": "OK"')
        print("http check availability (with proxy): %s" % avail)
    elif os.environ.get('PANDA_AUTH', None) and os.environ.get('PANDA_AUTH_VO', None) and os.environ.get('PANDA_AUTH_ID_TOKEN', None):
        curl = "curl -i -k -H \"X-IDDS-Auth-Type: ${PANDA_AUTH}\" -H \"X-IDDS-Auth-VO: ${PANDA_AUTH_VO}\" -H \"X-Idds-Auth-Token: ${PANDA_AUTH_ID_TOKEN}\" https://%s:8443/idds/ping" % host
        avail = check_command(curl, '"Status": "OK"')
        print("http check availability (with oidc token): %s" % avail)
    if not avail or avail == 0:
        curl = "curl -i -k https://%s:8443/idds/ping" % host
        avail = check_command(curl, 'IDDSException')
        print("http check availability (without proxy): %s" % avail)

    if not avail or avail == 0:
        logrotate_running = is_logrotate_running()
        restarting = is_restarting()
        if logrotate_running and restarting:
            print("log rotation is running and http is restarting")
            return 1
    return avail


def process_availability():
    # check the http
    process_avail = 0
    output = (
        subprocess.Popen(
            "ps -eo pgid,args | grep 'idds/agents/main.py' | grep -v grep | uniq",
            stdout=subprocess.PIPE,
            shell=True,
        )
        .communicate()[0]
        .decode("ascii")
    )
    count = 0
    for line in output.split("\n"):
        line = line.strip()
        if line == "":
            continue
        count += 1
    if count >= 1:
        process_avail = 100

    print("agent process check availability: %s" % process_avail)
    return process_avail


def heartbeat_availability(log_location):
    avail = 100
    hang_workers = 0
    heartbeat_file = os.path.join(log_location, 'idds_availability')
    if not os.path.exists(heartbeat_file):
        avail = 0
        print("idds_heartbeat at %s not exist, avail: %s" % (heartbeat_file, avail))
        return avail, hang_workers

    mod_time = os.path.getmtime(heartbeat_file)
    print("idds_heartbeat updated at %s (currently is %s, %s seconds ago)" % (mod_time, time.time(), time.time() - mod_time))
    if mod_time < time.time() - 1800:
        avail = 0
        return avail, hang_workers

    try:
        with open(heartbeat_file, 'r') as f:
            d = json.load(f)
            for agent in d:
                info = d[agent]
                num_hang_workers = info['num_hang_workers']
                num_active_workers = info['num_active_workers']
                if num_active_workers > 0 and num_hang_workers > 0:
                    hang_workers += num_hang_workers
                    agent_avail = int(num_hang_workers * 100 / num_active_workers)
                    if agent_avail < avail:
                        avail = agent_avail
                    print("iDDS agent %s has % hang workers" % num_hang_workers)
    except Exception as ex:
        print("Failed to parse idds_heartbeat: %s" % str(ex))
        avail = 50

    return avail, hang_workers


def idds_availability(host, log_location):
    infos = {}
    http_avail = http_availability(host)
    print(f"http avail: {http_avail}")

    process_avail = process_availability()
    print(f"agent daemon avail: {process_avail}")

    heartbeat_avail, hang_workers = heartbeat_availability(log_location)
    print(f"heartbeat avail: {heartbeat_avail}, hang workers: {hang_workers}")
    infos['num_hang_workers'] = hang_workers

    if not http_avail:
        availability = 0
        avail_info = "iDDS http rest service is not running"
    elif not process_avail:
        availability = 50
        avail_info = "iDDS agents are not running"
    else:
        if not heartbeat_avail:
            availability = 50
            avail_info = "iDDS agents are running. However heartbeat file is not found (or not renewed)"
        elif heartbeat_avail < 100:
            availability = heartbeat_avail
            avail_info = "iDDS agents are running. However there are hanging workers"
        else:
            availability = heartbeat_avail
            avail_info = "iDDS is OK"

    print("availability: %s, avail_info: %s, infos: %s" % (availability, avail_info, infos))

    return availability, avail_info, infos


def main():
    host = 'localhost'
    log_location = '/var/log/idds'
    avail, avail_info, infos = idds_availability(host, log_location)

    health_file = os.path.join(log_location, 'idds_health')
    if avail >= 100:
        with open(health_file, 'w') as f:
            f.write('OK')
    else:
        if os.path.exists(health_file):
            os.remove(health_file)


if __name__ == '__main__':
    main()
