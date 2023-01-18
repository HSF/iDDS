#!/usr/bin/env python

import argparse
import logging

import os
import glob
import string


def get_files(source):
    sources = []
    for name in glob.glob(source):
        sources.append(name)
    return sources


def get_file_template():
    template = """source s_${filename} {
    file("$source");
};
destination d_${filename} {
    file(
        "${destination}"
        template("$${ISODATE} ${flag} ${filename} $${HOST} $${MESSAGE}\\n"));
};
log { source(s_${filename}); destination(d_${filename}); };

"""
    return string.Template(template)


def get_pipe_template():
    template = """source s_${filename} {
    file("$source");
};
destination d_${filename} {
    pipe(
        "${destination}"
        template("$${ISODATE} ${flag} ${filename} $${HOST} $${MESSAGE}\\n"));
};
log { source(s_${filename}); destination(d_${filename}); };

"""
    return string.Template(template)


def generate_source_dest_pair(source, destination, flag, pipe=False):
    filename = os.path.basename(source).replace(".log", "").replace("_log", "")
    if pipe:
        template = get_pipe_template()
    else:
        template = get_file_template()
    ret = template.substitute(filename=filename, source=source, destination=destination, flag=flag)
    return ret


def generate_config(config_file, source, destination, flag, pipe=False):
    with open(config_file, 'w') as fd:
        sources = get_files(source)
        for src in sources:
            src_dest = generate_source_dest_pair(src, destination, flag, pipe)
            fd.write(src_dest)


logging.getLogger().setLevel(logging.INFO)
parser = argparse.ArgumentParser(description="Configure syslog-ng")
parser.add_argument('-s', '--source', default=None, help='Source files')
parser.add_argument('-d', '--destination', default=None, help='Destination file name')
parser.add_argument('-f', '--flag', default=None, help='Flag name')
parser.add_argument('-c', '--config', default=None, help='Configuration file to be generated')
parser.add_argument('-p', "--pipe", action="store_true", default=False, help='Use pipe')
args = parser.parse_args()


if __name__ == '__main__':
    generate_config(args.config, args.source, args.destination, args.flag, args.pipe)
