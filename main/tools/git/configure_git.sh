#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#


cp tools/git/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
cp tools/git/prepare-commit-msg .git/hooks/prepare-commit-msg
chmod +x .git/hooks/prepare-commit-msg
