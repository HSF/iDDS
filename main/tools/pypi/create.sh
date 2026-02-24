#!/bin/bash

CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
RootDir="$( dirname "$( dirname "$( dirname "$CurrentDir" )" )" )"

version=$1

# create packages
echo python ${RootDir}/build_all.py wheel
python ${RootDir}/build_all.py wheel

echo python3 -m twine upload  ${RootDir}/*/dist/idds*-${version}.tar.gz
python3 -m twine upload  ${RootDir}/*/dist/idds*-${version}.tar.gz

# python3 -m twine upload atlas/dist/idds-atlas-0.2.0.tar.gz
# python3 -m twine upload common/dist/idds-common-0.2.0.tar.gz
# python3 -m twine upload main/dist/idds-server-0.2.0.tar.gz
# python3 -m twine upload client/dist/idds-client-0.2.0.tar.gz
# python3 -m twine upload doma/dist/idds-doma-0.2.0.tar.gz
# python3 -m twine upload workflow/dist/idds-workflow-0.2.0.tar.gz
