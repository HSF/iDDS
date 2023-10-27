#!/usr/bin/python

"""
Cloned from panda_sls.py (https://gitlab.cern.ch/ai/it-puppet-hostgroup-vopanda/-/raw/master/code/files/pandaserver/panda_sls.py?ref_type=heads)
"""
import datetime
import json
import optparse
import os
import re
import smtplib
import socket
import subprocess
import time

from email.mime.text import MIMEText

import sls_document

###########################################
# define options
###########################################
parser = optparse.OptionParser()

parser.add_option(
    "--host",
    dest="host",
    type="string",
    help="Hostname of server to check, default is current machine hostname",
)
parser.add_option(
    "-l",
    "--logs",
    dest="logs",
    type="string",
    help="Location of the panda logs, where to look for information. Default is /var/log/idds",
)
parser.add_option(
    "--proxy",
    dest="proxy",
    type="string",
    help="Location of the X509 proxy. Default is $X509_USER_PROXY",
)
parser.add_option(
    "--debug",
    action="store_true",
    dest="debug",
    default=False,
    help="Print out debug statements.",
)

(options, args) = parser.parse_args()

# filepath to store the last email timestamps
# filename = os.path.join(os.path.expanduser("~"), "sls_notification_emails.txt")
# hardcode the path for now. The home directory is sometimes mapped to afs and the file conflicts between machines
filename = "/home/atlpilo1/sls_notification_emails.txt"


def __main__():
    if options.host:
        host = options.host
    else:
        host = socket.gethostname()
        host = re.sub(r"^(\w+).*", r"\1", host)

    if options.logs:  # Backwards compatibility. We plan to migrate the logs
        log_location = options.logs
    else:
        log_location = "/var/log/idds"

    if options.proxy:
        os.environ['X509_USER_PROXY'] = options.proxy

    make_idds(host, log_location)


def make_idds(host, log_location):
    if options.debug:
        print("Creating the idds monitoring entry")

    avail, avail_info, infos = idds_availability(host, log_location)

    sls_doc = sls_document.SlsDocument()
    id = "iDDS"
    sls_doc.set_id("%s_%s" % (id, host))
    sls_doc.set_status(avail)
    sls_doc.set_avail_desc(id)
    sls_doc.set_avail_info(avail_info)

    for key in infos:
        info = infos[key]
        sls_doc.add_data(key, info)

    email_manager(id, host, avail, avail_info)

    return sls_doc.send_document(options.debug)


def check_command(command, check_string):
    if options.debug:
        print("Checking command : {0}".format(command))
        print("For string : {0}".format(check_string))

    tmp_array = command.split()
    output = (
        subprocess.Popen(tmp_array, stdout=subprocess.PIPE)
        .communicate()[0]
        .decode("ascii")
    )

    if re.search(check_string, output):
        if options.debug:
            print("Found the string, return 100")
        return 100
    else:
        if options.debug:
            print("String not found, return 0")
        return 0


def http_availability(host):
    # check the http
    avail = 0
    if os.environ.get('X509_USER_PROXY', None):
        curl = "curl -i -k --cert $X509_USER_PROXY --key $X509_USER_PROXY --cacert $X509_USER_PROXY https://%s:443/idds/ping" % host
        avail = check_command(curl, '"Status": "OK"')
        if options.debug:
            print("http check availability (with proxy): %s" % avail)
    if not avail or avail == 0:
        curl = "curl -i -k https://%s:443/idds/ping" % host
        avail = check_command(curl, 'IDDSException')
        if options.debug:
            print("http check availability (without proxy): %s" % avail)
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

    if options.debug:
        print("agent process check availability: %s" % process_avail)
    return process_avail


def heartbeat_availability(log_location):
    avail = 100
    hang_workers = 0
    heartbeat_file = os.path.join(log_location, 'idds_availability')
    if not os.path.exists(heartbeat_file):
        avail = 0
        if options.debug:
            print("idds_heartbeat at %s not exist, avail: %s" % (heartbeat_file, avail))
        return avail, hang_workers

    mod_time = os.path.getmtime(heartbeat_file)
    if options.debug:
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
                    if options.debug:
                        print("iDDS agent %s has % hang workers" % num_hang_workers)
    except Exception as ex:
        print("Failed to parse idds_heartbeat: %s" % str(ex))
        avail = 50

    return avail, hang_workers


def idds_availability(host, log_location):
    infos = {}
    http_avail = http_availability(host)

    process_avail = process_availability()

    heartbeat_avail, hang_workers = heartbeat_availability(log_location)
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

    if options.debug:
        print("availability: %s, avail_info: %s, infos: %s" % (availability, avail_info, infos))

    return availability, avail_info, infos


def read_last_email_times():
    try:
        with open(filename, "r") as f:
            lines = f.readlines()
        return [
            datetime.datetime.strptime(line.strip(), "%Y-%m-%d %H:%M:%S")
            for line in lines
        ]
    except FileNotFoundError:
        return []


def update_email_times(timestamps):
    with open(filename, "w+") as f:
        # Save the last 10 timestamps
        for ts in timestamps[-10:]:
            f.write(ts.strftime("%Y-%m-%d %H:%M:%S") + "\n")


def send_email(subject, body, to_email):
    from_email = "atlpan@mail.cern.ch"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    server = smtplib.SMTP("localhost")
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()


def email_manager(service, host, avail, avail_info):
    try:
        # If the server is 100% available, then skip the email
        if avail in ("100", 100):
            return

        # Get the last email times. Don't send more than one email per hour
        last_email_times = read_last_email_times()
        now = datetime.datetime.now()
        if last_email_times and now - last_email_times[-1] <= datetime.timedelta(
            hours=1
        ):
            return

        # Email subject
        subject = "[SLS] Service issues for {0} on {1}".format(service, host)

        # Email content
        body = """
        Service: {0}
        Host: {1}
        Availability: {2}
        Availability info: {3}
        """.format(
            service, host, avail, avail_info
        )

        email = "atlas-adc-idds-admins@cern.ch"

        send_email(subject, body, email)

        # Record the time of the email
        last_email_times.append(now)
        update_email_times(last_email_times)

        if options.debug:
            print("Email sent.")
        return

    except Exception:
        pass


# run program
__main__()
