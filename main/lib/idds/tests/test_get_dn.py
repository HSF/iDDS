#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@@cern.ch>, 2021 - 2022

import re

from idds.common import authentication


# /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=wguan/CN=667815/CN=Wen Guan/CN=1883443395
def get_user_name_from_dn1(dn):
    try:
        up = re.compile('/(DC|O|OU|C|L)=[^\/]+')        # noqa W605
        username = up.sub('', dn)
        up2 = re.compile('/CN=[0-9]+')
        username = up2.sub('', username)
        up4 = re.compile(' [0-9]+')
        username = up4.sub('', username)
        up5 = re.compile('_[0-9]+')
        username = up5.sub('', username)
        username = username.replace('/CN=proxy', '')
        username = username.replace('/CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub('/CN=Robot:[^/]+,', ',', username)
        username = re.sub('/CN=Robot:[^/]+', '', username)
        username = re.sub('/CN=Robot[^/]+,', ',', username)
        username = re.sub('/CN=Robot[^/]+', '', username)
        username = re.sub('/CN=nickname:[^/]+,', ',', username)
        username = re.sub('/CN=nickname:[^/]+', '', username)
        pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')         # noqa W605
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace('/CN=', '')
        if username.lower().find('/email') > 0:
            username = username[:username.lower().find('/email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        return username
    except Exception:
        return dn


# 'CN=203633261,CN=Wen Guan,CN=667815,CN=wguan,OU=Users,OU=Organic Units,DC=cern,DC=ch'
def get_user_name_from_dn2(dn):
    try:
        up = re.compile(',(DC|O|OU|C|L)=[^\,]+')        # noqa W605
        username = up.sub('', dn)
        up2 = re.compile(',CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile('CN=[0-9]+,')
        username = up3.sub(',', username)
        up4 = re.compile(' [0-9]+')
        username = up4.sub('', username)
        up5 = re.compile('_[0-9]+')
        username = up5.sub('', username)
        username = username.replace(',CN=proxy', '')
        username = username.replace(',CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub(',CN=Robot:[^/]+,', ',', username)
        username = re.sub(',CN=Robot:[^/]+', '', username)
        username = re.sub(',CN=Robot[^/]+,', ',', username)
        username = re.sub(',CN=Robot[^/]+', '', username)
        username = re.sub(',CN=nickname:[^/]+,', ',', username)
        username = re.sub(',CN=nickname:[^/]+', '', username)
        pat = re.compile('.*,CN=([^\,]+),CN=([^\,]+)')         # noqa W605
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        else:
            username = username.replace(',CN=', '')
        if username.lower().find(',email') > 0:
            username = username[:username.lower().find(',email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        return username
    except Exception:
        return dn


def get_user_name_from_dn(dn):
    dn = get_user_name_from_dn1(dn)
    dn = get_user_name_from_dn2(dn)
    return dn


if __name__ == '__main__':
    dn = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=wguan/CN=667815/CN=Wen Guan/CN=1883443395"
    username = get_user_name_from_dn(dn)
    print(username)
    username = authentication.get_user_name_from_dn(dn)
    print("auth: " + username)

    dn = 'CN=203633261,CN=Wen Guan,CN=667815,CN=wguan,OU=Users,OU=Organic Units,DC=cern,DC=ch'
    username = get_user_name_from_dn(dn)
    print(username)
    username = authentication.get_user_name_from_dn(dn)
    print("auth: " + username)

    dn = "/DC=ch/DC=cern/OU=Organic+Units/OU=Users/CN=atlpilo1/CN=614260/CN=Robot%3A+ATLAS+Pilot1"
    username = get_user_name_from_dn(dn)
    print(username)
    username = authentication.get_user_name_from_dn(dn)
    print("auth: " + username)

    dn = "CN=1316551436,CN=Robot: ATLAS Panda Server1,CN=663551,CN=pandasv1,OU=Users,OU=Organic Units,DC=cern,DC=ch"
    username = get_user_name_from_dn(dn)
    print(username)
    username = authentication.get_user_name_from_dn(dn)
    print("auth: " + username)
