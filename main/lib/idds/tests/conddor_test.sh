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

condor_submit /tmp/test_condor.jdl
