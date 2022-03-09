#!/usr/bin/env python

import re


def match_pattern_file(pattern, lfn):
    pattern1 = "\\$[_a-zA-Z0-9]+"
    pattern2 = "\\$\\{[_a-zA-Z0-9\\/]+\\}"
    while True:
        m = re.search(pattern1, pattern)
        if m:
            pattern = pattern.replace(m.group(0), "*")
        else:
            break
    while True:
        m = re.search(pattern2, pattern)
        if m:
            pattern = pattern.replace(m.group(0), "*")
        else:
            break
    pattern = pattern.replace(".", "\\.")
    pattern = pattern.replace("*", ".*")
    pattern = pattern + "$"
    print(pattern)

    m = re.search(pattern, lfn)
    if m:
        return True
    return False


if __name__ == "__main__":
    pattern = 'user.tmaeno.$JEDITASKID._${SN/P}.results.json'
    lfn = 'user.tmaeno.2321._23.results.json'
    lfn1 = 'user.tmaeno1.2321._23.results.json'
    lfn2 = 'user.tmaeno.2321._23.results1.json'
    lfn3 = 'user.tmaeno.2321._23.results.json1'
    lfn4 = 'user.tmaeno.27529814._000001.results.json'

    ret = match_pattern_file(pattern, lfn)
    print(ret)
    ret = match_pattern_file(pattern, lfn1)
    print(ret)
    ret = match_pattern_file(pattern, lfn2)
    print(ret)
    ret = match_pattern_file(pattern, lfn3)
    print(ret)
    ret = match_pattern_file(pattern, lfn4)
    print(ret)
