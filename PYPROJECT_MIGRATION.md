# iDDS Build System Migration to pyproject.toml

This document describes the migration from `setup.py` to modern `pyproject.toml` configuration.

## Overview

The iDDS project has been migrated from the legacy `setup.py` approach to modern `pyproject.toml` configuration (PEP 517/518/621). This provides:

- **Standard configuration**: All metadata in declarative `pyproject.toml` files
- **Better tooling**: Native support in pip, build, and modern Python tools
- **Reproducible builds**: Isolated build environments
- **Development workflow**: Simplified installation and dependency management

## Structure

The repository contains multiple packages:

- `common/` - Core utilities and common code
- `main/` - Server components
- `client/` - Client library
- `atlas/` - ATLAS-specific extensions
- `doma/` - DOMA extensions
- `workflow/` - Workflow management
- `prompt/` - Prompt agent
- `website/` - Web interface
- `monitor/` - Monitoring components

Each package now has its own `pyproject.toml` file.

## Installation

### Option 1: Using the build script (recommended)

```bash
# Install all packages in development mode
python build_all.py develop

# Or install normally
python build_all.py install

# Build wheel distributions
python build_all.py build
```

### Option 2: Using make

```bash
# Install in development mode
make develop

# Or install normally
make install

# Build wheels
make build

# Clean build artifacts
make clean
```

### Option 3: Manual installation

Install packages individually in dependency order:

```bash
# Install common package first (required by others)
cd common && pip install -e . && cd ..

# Then install other packages
cd main && pip install -e . && cd ..
cd client && pip install -e . && cd ..
# ... etc
```

## Development Workflow

### Setting up for development

```bash
# Clone the repository
git clone https://github.com/HSF/iDDS.git
cd iDDS

# Install in development/editable mode
python build_all.py develop

# Or use make
make develop
```

### Running tests

```bash
# Run all tests
pytest

# Or use make
make test

# Run tests for a specific package
cd common && pytest
```

### Code quality

```bash
# Run linting
make lint

# Format code with black
make format
```

## Building Distributions

### Build wheels for all packages

```bash
python build_all.py wheel
# Wheels will be in each package's dist/ directory
```

### Build a specific package

```bash
cd common
python -m build
# or
pip wheel .
```

## Migration Notes

### What Changed

1. **Configuration files**: Each package now has `pyproject.toml` instead of `setup.py`
2. **Build command**: Use `python build_all.py <command>` instead of `python setup.py <command>`
3. **Dependencies**: Declared in `dependencies` array in `pyproject.toml`
4. **Custom commands**: Removed custom install/wheel commands (no longer needed)

### Backwards Compatibility

The old `setup.py` files are kept for reference but are deprecated. Use the new `pyproject.toml` approach.

### Key Differences

| Old (setup.py) | New (pyproject.toml) |
|---------------|---------------------|
| `python setup.py install` | `pip install .` or `python build_all.py install` |
| `python setup.py develop` | `pip install -e .` or `python build_all.py develop` |
| `python setup.py bdist_wheel` | `python -m build --wheel` or `python build_all.py wheel` |
| `python setup.py clean` | `python build_all.py clean` |

## Package Dependencies

```
idds-common (base)
├── idds-server (main)
├── idds-client
├── idds-atlas
├── idds-doma
├── idds-workflow
├── idds-prompt
├── idds-website
└── idds-monitor
```

All packages depend on `idds-common`. Install it first if installing manually.

## Troubleshooting

### Build package not found

```bash
pip install build
```

### Import errors after migration

Reinstall in development mode:

```bash
python build_all.py clean
python build_all.py develop
```

### Namespace package conflicts

The migration handles namespace packages correctly. Each subpackage (e.g., `idds.common`, `idds.core`) is properly configured to work together.

## Tools Configuration

The root `pyproject.toml` contains shared tool configurations:

- **Black**: Code formatter (line-length: 100)
- **pytest**: Test runner with coverage
- **mypy**: Type checking
- **flake8**: Linting

These apply to all packages in the monorepo.

## CI/CD Integration

Update your CI/CD pipelines to use the new build system:

```yaml
# Example GitHub Actions
- name: Install dependencies
  run: |
    python -m pip install --upgrade pip build
    
- name: Install iDDS packages
  run: python build_all.py develop
  
- name: Build distributions
  run: python build_all.py wheel
```

## References

- [PEP 517](https://peps.python.org/pep-0517/) - Build system interface
- [PEP 518](https://peps.python.org/pep-0518/) - pyproject.toml
- [PEP 621](https://peps.python.org/pep-0621/) - Project metadata
- [Setuptools Pyproject Guide](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html)
