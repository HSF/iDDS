#!/bin/bash

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Current dir: `pwd`"
echo "Curent dir contents:"
ls

echo "PATH: $PATH"

# install iDDS client
cd client
# python setup.py install --force --prefix $CurrentDir
python setup.py install --old-and-unmanageable --force --prefix $CurrentDir
cd ..

cd common
python setup.py install --old-and-unmanageable --force --prefix $CurrentDir
cd ..

cd workflow
python setup.py install --old-and-unmanageable --force --prefix $CurrentDir
cd ..

cd doma
python setup.py install --old-and-unmanageable --force --prefix $CurrentDir
cd ..

echo "Current dir: `pwd`"
echo "Curent dir contents:"
ls

python_install_path=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(prefix='$CurrentDir'))")

export PYTHONPATH=${python_install_path}:$PYTHONPATH

echo IDDS_BUILD_REQUEST_ID=$IDDS_BUILD_REQUEST_ID
echo IDDS_BUIL_SIGNATURE=$IDDS_BUIL_SIGNATURE

if [[ ! -z "${PANDA_AUTH_DIR}" ]] && [[ ! -z "${PANDA_AUTH_ORIGIN}" ]]; then
    export PANDA_AUTH_ID_TOKEN=$(cat $PANDA_AUTH_DIR);
    export PANDA_AUTH_VO=$PANDA_AUTH_ORIGIN;
    export IDDS_OIDC_TOKEN=$(cat $PANDA_AUTH_DIR)
    export IDDS_VO=$PANDA_AUTH_ORIGIN
    export PANDA_AUTH=oidc
    export IDDS_AUTH_TYPE=oidc
else
    unset PANDA_AUTH;
    export IDDS_AUTH_TYPE=x509_proxy
fi;

# echo PYTHONPATH=$PYTHONPATH
# which python
# which python3

echo "envs: "
env

export IDDS_LOG_LEVEL=DEBUG


echo "exec command: python test_domapanda_build.py $@"
python  test_domapanda_build.py $@
