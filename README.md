iDDS
====

iDDS is an intelligent Distributed Dispatch and Scheduling Service. It's designed to transform and deliver
data for High Energy Physics workloads.

## Package Structure

iDDS is organized as a monorepo with multiple packages:

```
idds-common (base utilities)
├── idds-server (main server components)
├── idds-client (client library)
│   └── requires: idds-workflow
├── idds-workflow (workflow management)
├── idds-atlas (ATLAS-specific extensions)
│   └── requires: idds-workflow
├── idds-doma (DOMA extensions)
│   └── requires: idds-workflow
├── idds-prompt (prompt agent)
├── idds-website (web interface)
└── idds-monitor (monitoring components)
```

All packages depend on `idds-common` as the base layer.

## Installation

### Using the build script (recommended)

```bash
# Install all packages in development mode
python3 build_all.py develop

# Or install normally
python3 build_all.py install

# Build wheel distributions
python3 build_all.py build
```

### Using make

```bash
make develop    # Install in development mode
make install    # Install normally
make build      # Build wheels
make clean      # Clean build artifacts
```

See [PYPROJECT_MIGRATION.md](PYPROJECT_MIGRATION.md) for detailed installation instructions.

Home page
---------
https://iddsserver.cern.ch/website/


Documents
---------
https://idds.readthedocs.io (dev)


PanDA Monitor
-------------
https://bigpanda.cern.ch/idds/ 
