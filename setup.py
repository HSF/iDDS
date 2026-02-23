#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

"""
Legacy setup.py wrapper for backward compatibility.
Please use build_all.py or pyproject.toml-based builds instead.
"""

import os
import subprocess
import sys


def setup(argv):
    """
    Wrapper around build_all.py for backward compatibility.
    Maps old setup.py commands to new build_all.py commands.
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    build_script = os.path.join(current_dir, 'build_all.py')
    
    # Map old setup.py commands to new build_all.py commands
    command_map = {
        'install': 'install',
        'develop': 'develop',
        'build': 'build',
        'bdist_wheel': 'wheel',
        'sdist': 'build',
        'clean': 'clean',
    }
    
    # Parse the command from argv
    if len(argv) > 0:
        old_command = argv[0]
        new_command = command_map.get(old_command, None)
        
        if new_command:
            cmd = f'python {build_script} {new_command}'
            print(f"Running: {cmd}")
            print(f"(Mapped from setup.py {old_command})")
            return subprocess.call(cmd, shell=True)
        else:
            print(f"Warning: Command '{old_command}' not supported.")
            print("Please use build_all.py directly or one of: install, develop, build, wheel, clean")
            return 1
    else:
        # No command specified, show help
        cmd = f'python {build_script}'
        print("No command specified. Running build_all.py...")
        print(f"Usage: python setup.py [install|develop|build|bdist_wheel|clean]")
        print(f"Or use directly: python build_all.py [command]")
        return subprocess.call(cmd, shell=True)


if __name__ == '__main__':
    sys.exit(setup(sys.argv[1:]))

