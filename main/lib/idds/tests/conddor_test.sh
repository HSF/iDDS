#!/bin/bash

rm -fr /tmp/test_condor.jdl
cat <<EOT >> /tmp/test_condor.jdl
    Universe   = vanilla
    Initialdir = /tmp
    Executable = /bin/hostname
    output     = test.out
    error      = test.err
    log        = test.log
    queue
EOT

#condor_submit /tmp/test_condor.jdl
condor_submit --debug -name aipanda180.cern.ch -pool aipanda180.cern.ch:9618 /tmp/test_condor.jdl
