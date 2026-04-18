#!/usr/bin/env python
"""
Build script to install all iDDS packages using pyproject.toml configuration.
This replaces the old setup.py meta-installer.

Usage:
    python build_all.py install         # Install all packages
    python build_all.py develop         # Install in development mode
    python build_all.py build           # Build all packages
    python build_all.py clean           # Clean build artifacts
    python build_all.py wheel           # Build wheel distributions
"""

import os
import subprocess
import sys
from pathlib import Path


# List of packages in installation order (dependencies first)
PACKAGES = [
    'common',
    'main',
    'client',
    'atlas',
    'workflow',
    'doma',
    'website',
    'monitor',
    'prompt',
]


def run_command(cmd, cwd):
    """Run a command in the specified directory."""
    print(f"\n{'='*60}")
    print(f"Running: {cmd}")
    print(f"In directory: {cwd}")
    print(f"{'='*60}\n")
    
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        capture_output=False,
        text=True
    )
    
    if result.returncode != 0:
        print(f"ERROR: Command failed with return code {result.returncode}")
        return False
    return True


def clean_package(package_path):
    """Clean build artifacts for a package."""
    dirs_to_remove = ['build', 'dist', '*.egg-info', '__pycache__']
    
    for pattern in dirs_to_remove:
        if '*' in pattern:
            import glob
            for path in glob.glob(str(package_path / pattern)):
                print(f"Removing: {path}")
                if os.path.isdir(path):
                    import shutil
                    shutil.rmtree(path, ignore_errors=True)
        else:
            path = package_path / pattern
            if path.exists():
                print(f"Removing: {path}")
                import shutil
                shutil.rmtree(path, ignore_errors=True)


def fix_alembic_ini_python_version():
    """Replace {python_version} placeholder in the installed config_default/alembic.ini with the running Python version."""
    import re
    alembic_ini = Path(sys.prefix) / 'config_default' / 'alembic.ini'
    if not alembic_ini.exists():
        print(f"WARNING: installed alembic.ini not found at {alembic_ini}, skipping python version fix")
        return
    python_ver = 'python%d.%d' % sys.version_info[:2]
    content = alembic_ini.read_text()
    new_content = content.replace('{python_version}', python_ver)
    new_content = re.sub(r'python3\.\d+', python_ver, new_content)
    if new_content != content:
        alembic_ini.write_text(new_content)
        print(f"Updated {alembic_ini}: python version set to {python_ver}")


def process_packages(command):
    """Process all packages with the given command."""
    current_dir = Path(__file__).parent.resolve()
    success_count = 0

    # Map user commands to pip/build commands
    command_map = {
        'install': 'pip install .',
        'develop': 'pip install -e .',
        'build': 'python -m build',
        'wheel': 'python -m build --wheel',
    }
    
    if command == 'clean':
        print("Cleaning build artifacts...")
        for package in PACKAGES:
            package_path = current_dir / package
            if package_path.exists():
                clean_package(package_path)
        print("\nClean completed for all packages!")
        return
    
    if command not in command_map:
        print(f"Unknown command: {command}")
        print(f"Available commands: {', '.join(list(command_map.keys()) + ['clean'])}")
        sys.exit(1)
    
    pip_command = command_map[command]
    
    for package in PACKAGES:
        package_path = current_dir / package
        
        if not package_path.exists():
            print(f"WARNING: Package directory not found: {package_path}")
            continue
        
        if not (package_path / 'pyproject.toml').exists():
            print(f"WARNING: No pyproject.toml found in {package}, skipping...")
            continue
        
        success = run_command(pip_command, package_path)
        
        if success:
            success_count += 1
            print(f"✓ {package} completed successfully")
        else:
            print(f"✗ {package} failed")
            if input(f"\nContinue with remaining packages? (y/n): ").lower() != 'y':
                break
    
    if command in ('install', 'develop'):
        fix_alembic_ini_python_version()

    print(f"\n{'='*60}")
    print(f"Completed: {success_count}/{len(PACKAGES)} packages")
    print(f"{'='*60}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    
    # Check if pip and build are available
    if command in ['build', 'wheel']:
        try:
            import build  # noqa: F401
        except ImportError:
            print("ERROR: 'build' package not found. Install it with:")
            print("    pip install build")
            sys.exit(1)
    
    process_packages(command)


if __name__ == '__main__':
    main()
