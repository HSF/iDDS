#!/bin/bash

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ToolsWorkflowDir="$( dirname "$CurrentDir" )"
ToolsDir="$( dirname "$ToolsWorkflowDir" )"
WorkflowDir="$( dirname "$ToolsDir" )"
RootDir="$( dirname "$WorkflowDir" )"

EXECNAME=${WorkflowDir}/bin/run_workflow_wrapper
rm -fr $EXECNAME

workdir=/tmp/idds
tmpzip=/tmp/idds/tmp.zip
rm -fr $workdir
mkdir -p $workdir

echo "setup virtualenv $workdir"
# source /afs/cern.ch/user/w/wguan/workdisk/conda/setup_mini.sh
# conda deactivate
# echo conda env create --prefix=$workdir -f ${WorkflowDir}/tools/workflow/make/environment.yaml
# conda env create --prefix=$workdir -f ${WorkflowDir}/tools/workflow/make/environment.yaml
# conda activate $workdir

python3 -m venv $workdir
source $workdir/bin/activate

echo "install panda client"
pip install panda-client
# pip install tabulate requests urllib3==1.26.18 argcomplete packaging anytree networkx stomp.py==8.0.1
pip install tabulate requests urllib3 argcomplete packaging anytree networkx stomp.py wheel

echo "install idds-common"
python ${RootDir}/common/setup.py clean --all
python ${RootDir}/common/setup.py install --old-and-unmanageable --force

echo "install idds-client"
python ${RootDir}/client/setup.py clean --all
python ${RootDir}/client/setup.py install --old-and-unmanageable --force

echo "install idds-workflow"
python ${RootDir}/workflow/setup.py clean --all
python ${RootDir}/workflow/setup.py install --old-and-unmanageable --force

python_lib_path=`python -c 'from sysconfig import get_path; print(get_path("purelib"))'`
echo $python_lib_path

cur_dir=$PWD

# cd ${python_lib_path}
# # for libname in idds pandaclient pandatools tabulate pyjwt requests urllib3 argcomplete cryptography packaging anytree networkx; do
# for libname in idds pandaclient pandatools tabulate jwt requests urllib3 argcomplete cryptography packaging stomp; do
#     echo zip -r $tmpzip  $libname
#     zip -r $tmpzip  $libname
# done
# cd -

cd $workdir
mkdir lib_py
# for libname in idds pandaclient pandatools tabulate pyjwt requests urllib3 argcomplete cryptography packaging anytree networkx; do
# for libname in idds pandaclient pandatools tabulate jwt requests urllib3 argcomplete cryptography packaging stomp cffi charset_normalizer docopt.py idna pycparser six.py websocket _cffi_backend*; do
for libname in idds pandaclient pandatools tabulate requests urllib3 argcomplete stomp websocket charset_normalizer idna certifi packaging; do
    echo cp -fr ${python_lib_path}/$libname lib_py
    cp -fr ${python_lib_path}/$libname lib_py
done
echo zip -r $tmpzip lib_py
zip -r $tmpzip lib_py
cd -

cd $workdir
echo zip -r $tmpzip  etc
zip -r $tmpzip  etc
cd -

cd ${WorkflowDir}
echo zip -r $tmpzip  bin
zip -r $tmpzip  bin

cd -

cat ${WorkflowDir}/tools/workflow/make/zipheader $tmpzip > $EXECNAME
chmod +x $EXECNAME
