#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2021 - 2022

"""
Setup Panda IAM token
"""

from __future__ import print_function

import argparse
import argcomplete
import base64
import datetime
import json
import os
import socket
import subprocess
import sys

try:
    from urllib import urlencode
    from urllib2 import urlopen, Request, HTTPError
except ImportError:
    from urllib.request import urlopen, Request
    from urllib.parse import urlencode
    from urllib.error import HTTPError

from pandaclient import panda_api
from pandaclient import Client
# from pandaclient import openidc_utils
from pandaclient import PLogger


def run_command(cmd):
    """
    Runs a command in an out-of-procees shell.
    """
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    stdout, stderr = process.communicate()
    if stdout is not None and type(stdout) in [bytes]:
        stdout = stdout.decode()
    if stderr is not None and type(stderr) in [bytes]:
        stderr = stderr.decode()
    status = process.returncode
    return status, stdout, stderr


def setup_panda_token(verbose=False):
    c = panda_api.get_api()
    c.hello(verbose)


def get_expire_time(verbose=False):
    try:
        # token_file = openidc_utils.OpenIdConnect_Utils().get_token_path()
        curl = Client._Curl()
        curl.verbose = verbose
        tmp_log = PLogger.getPandaLogger()
        oidc = curl.get_oidc(tmp_log)
        token_file = oidc.get_token_path()
        if os.path.exists(token_file):
            with open(token_file) as f:
                data = json.load(f)
                enc = data['id_token'].split('.')[1]
                enc += '=' * (-len(enc) % 4)
                dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
                exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
                delta = exp_time - datetime.datetime.utcnow()
                minutes = delta.total_seconds() / 60
                print('Token will expire in %s minutes.' % minutes)
                print('Token expiration time : {0} UTC'.format(exp_time.strftime("%Y-%m-%d %H:%M:%S")))
        else:
            print("Cannot find token file.")
    except Exception as e:
        print('failed to decode cached token with {0}'.format(e))


def get_token_info(verbose=False):
    # c = panda_api.get_api()
    curl = Client._Curl()
    curl.verbose = verbose
    token_info = curl.get_token_info()
    # print(token_info)
    if token_info and type(token_info) in [dict]:
        for key in token_info:
            print("%s: %s" % (key, token_info[key]))
        get_expire_time()
    else:
        print(token_info)


def get_refresh_token_string(verbose=False):
    try:
        curl = Client._Curl()
        curl.verbose = verbose
        tmp_log = PLogger.getPandaLogger()
        oidc = curl.get_oidc(tmp_log)
        token_file = oidc.get_token_path()
        if os.path.exists(token_file):
            with open(token_file) as f:
                data = json.load(f)
                enc = data['id_token'].split('.')[1]
                enc += '=' * (-len(enc) % 4)
                dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
                exp_time = datetime.datetime.utcfromtimestamp(dec['exp'])
                delta = exp_time - datetime.datetime.utcnow()
                minutes = delta.total_seconds() / 60
                print('Token will expire in %s minutes.' % minutes)
                print('Token expiration time : {0} UTC'.format(exp_time.strftime("%Y-%m-%d %H:%M:%S")))
                if delta < datetime.timedelta(minutes=0):
                    print("Token already expired. Cannot refresh.")
                    return False, None, None
                return True, data['refresh_token'], delta
        else:
            print("Cannot find token file.")
    except Exception as e:
        print('failed to decode cached token with {0}'.format(e))
    return False, None, None


def oidc_refresh_token(oidc, token_endpoint, client_id, client_secret, refresh_token_string):
    if oidc.verbose:
        oidc.log_stream.debug('refreshing token')
    data = {'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token_string}
    rdata = urlencode(data).encode()
    req = Request(token_endpoint, rdata)
    req.add_header('content-type', 'application/x-www-form-urlencoded')
    try:
        conn = urlopen(req)
        text = conn.read()
        if oidc.verbose:
            oidc.log_stream.debug(text)
        id_token = json.loads(text)['id_token']
        text = json.dumps(json.loads(text))
        with open(oidc.get_token_path(), 'w') as f:
            f.write(text)
        return True, id_token
    except HTTPError as e:
        return False, 'code={0}. reason={1}. description={2}'.format(e.code, e.reason, e.read())
    except Exception as e:
        return False, str(e)


def refresh_token(minutes=30, verbose=False):
    curl = Client._Curl()
    curl.verbose = verbose
    tmp_log = PLogger.getPandaLogger()
    oidc = curl.get_oidc(tmp_log)

    status, refresh_token, delta = get_refresh_token_string()
    if not status:
        print("Cannot refresh token.")
        return False

    print("Fetching auth configuration from: %s" % str(oidc.auth_config_url))
    s, o = oidc.fetch_page(oidc.auth_config_url)
    if not s:
        print("Failed to get Auth configuration: " + o)
        return False
    auth_config = o

    print("Fetching endpoint configuration from: %s" % str(auth_config['oidc_config_url']))
    s, o = oidc.fetch_page(auth_config['oidc_config_url'])
    if not s:
        print("Failed to get endpoint configuration: " + o)
        return False
    endpoint_config = o

    # s, o = oidc.refresh_token(endpoint_config['token_endpoint'], auth_config['client_id'],
    #                           auth_config['client_secret'], refresh_token)
    s, o = oidc_refresh_token(oidc, endpoint_config['token_endpoint'], auth_config['client_id'],
                              auth_config['client_secret'], refresh_token)
    if not s:
        print("Failed to refresh token: " + o)
        if delta < datetime.timedelta(minutes=minutes):
            print("The left lifetime of the token is less than required %s minutes" % minutes)
            return False
        else:
            return True
    else:
        print("Success to refresh token: " + o)
        if delta < datetime.timedelta(minutes=minutes):
            print("The left lifetime of the token is less than required %s minutes" % minutes)
            return False
        else:
            return True
    return True


def refresh_token_action(min_minutes, email, stop_idds, verbose=False):
    status = refresh_token(min_minutes, verbose=verbose)
    if not status:
        if email:
            hostname = socket.getfqdn()
            cmd = """sendmail "%s"<<EOF
                     Subject: iDDS token at %s is going to expire.

                     iDDS token is googing to expire. Please refresh it.
                     EOF
                     """
            cmd = cmd % (email, hostname)
            status, stdout, stderr = run_command(cmd)
            print("Notifying %s: status: %s, stdout: %s, stderr: %s" % (email, status, stdout, stderr))
        if stop_idds:
            cmd = "supervisorctl start all"
            status, stdout, stderr = run_command(cmd)
            print("Stop idds %s: status: %s, stdout: %s, stderr: %s" % (cmd, status, stdout, stderr))


def get_parser():
    """
    Return the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    # subparsers = oparser.add_subparsers()

    oparser.add_argument('--setup', '-s', default=False, action='store_true', help="Setup token.")
    oparser.add_argument('--info', '-i', default=False, action='store_true', help="Print token info.")
    oparser.add_argument('--refresh', '-r', default=False, action='store_true', help="Refresh token.")
    oparser.add_argument('--verbose', '-v', default=False, action='store_true', help="Verbose.")
    oparser.add_argument('--panda_config_root', '-p', default=None, action='store', help="Panda config root path. If it's not specified and PANDA_CONFIG_ROOT is not defined, ~/.panda/ will be used.")
    oparser.add_argument('--email', '-e', default=None, action='store', help="Email to notify when failing to refresh token and the token will expire.")
    oparser.add_argument('--stop_idds', '-t', default=False, action='store_true', help="Stop idds when failing to refresh token and the token will expire.")
    oparser.add_argument('--min_minutes', '-m', default=35, action='store', type=int, help="Minimal minutes for a token before email notification and stop idds.")

    return oparser


if __name__ == '__main__':
    oparser = get_parser()
    argcomplete.autocomplete(oparser)

    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(-1)

    arguments = sys.argv[1:]
    args = oparser.parse_args(arguments)
    if args.panda_config_root:
        os.environ["PANDA_CONFIG_ROOT"] = args.panda_config_root
    else:
        if "PANDA_CONFIG_ROOT" not in os.environ:
            os.environ["PANDA_CONFIG_ROOT"] = "~/.panda"
    if not os.path.exists(os.environ["PANDA_CONFIG_ROOT"]):
        os.makedirs(os.environ["PANDA_CONFIG_ROOT"])

    if args.setup:
        setup_panda_token(args.verbose)
    elif args.info:
        get_token_info(args.verbose)
    elif args.refresh:
        refresh_token_action(min_minutes=args.min_minutes, email=args.email, stop_idds=args.stop_idds, verbose=args.verbose)
    else:
        oparser.print_help()
        sys.exit(-1)
