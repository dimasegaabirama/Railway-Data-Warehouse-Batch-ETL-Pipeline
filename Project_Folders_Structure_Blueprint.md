# Project Folder Structure Blueprint

Updated: 2026-11-02

## Project Overview

This repository is a single Python batch data-pipeline project with service-specific Docker Compose stacks. Its code follows a layered and medallion organization: application orchestration calls core configuration and registry services, which coordinate extract, transform, quality-check, and load modules across Bronze, Silver, and Gold.

Airflow and Spark are separate runtime services, not separate source repositories. Both depend on the `src` package and its current import root. The Spark submit container mounts `src/`, `config/`, and `.env.global`; the Jupyter container mounts `src/notebooks/`.

## Directory Tree

Generated environments and caches such as `.git/`, `.venv/`, `.ruff_cache/`, `__pycache__/`, `.ipynb_checkpoints/`, and `*.egg-info/` are omitted.

```text
.
|-- airflow/
|   `-- dags/
|       `-- train_pipeline.py
|-- config/
|   `-- pipeline-config.yaml
|-- docker/
|   |-- airflow/          # Airflow Compose files and import stubs
|   |-- hadoop/           # Hadoop services
|   |-- mongo/            # MongoDB and initialization data
|   |-- nessie/           # Nessie catalog and metadata database
|   `-- spark/            # Spark Compose files, defaults, and images
|-- images/               # Architecture and service screenshots
|-- skills/
|   |-- folder-structure/ # Structure blueprint guidance
|   `-- refactor/         # Refactoring guidance
|-- src/
|   |-- app/              # Pipeline and bootstrap entry points
|   |-- core/             # Configuration, registry, session, logging
|   |   `-- config/       # Config loading and domain managers
|   |-- data_quality/     # PyDeequ checks by medallion stage
|   |-- etl/
|   |   |-- extract/      # MongoDB and Iceberg readers
|   |   |-- load/         # Iceberg writers
|   |   `-- transform/    # Bronze, Silver, and Gold transforms
|   |-- models/           # Pydantic configuration and result models
|   |-- notebooks/        # Jupyter notebooks mounted by Spark Jupyter
|   `-- utils/            # Shared filtering, table, text, and Nessie helpers
|-- .env.example          # Non-secret environment template
|-- pyproject.toml        # Package metadata, supported Python, dependencies, Ruff
|-- requirements.txt      # pip fallback dependencies
|-- uv.lock               # Locked dependency resolution
|-- start-all.sh          # Start service stacks
`-- stop-all.sh           # Stop service stacks
```

Current file counts (excluding generated caches): `src/` 83, `docker/` 16, `images/` 6, `skills/` 2, `airflow/` 1, and `config/` 1. Counts change as files are added.

## Directory Responsibilities

| Path | Responsibility | Placement convention |
| --- | --- | --- |
| `src/app/` | CLI entry points and orchestration | Add application workflows here; keep reusable logic in `core/` or `etl/`. |
| `src/core/` | Shared configuration, session setup, logging, registry | Add cross-stage infrastructure here, not table-specific business rules. |
| `src/core/config/manager/` | Typed access to pipeline, table, source, catalog, and Spark settings | Keep configuration lookup in managers rather than scattering YAML parsing. |
| `src/etl/extract/` | Source-specific reads | Add a source reader and register it in `src/core/registry.py`. |
| `src/etl/transform/{bronze,silver,gold}/` | Stage and table transformations | Use snake_case filenames and return the shared transform-result model. |
| `src/etl/load/` | Writes to the lakehouse | Keep destination-specific write behavior in a loader. |
| `src/data_quality/{bronze,silver,gold}/` | PyDeequ validation suites | Name checks `test_<table>.py` and register them in the DQ registry. |
| `config/` | Declarative schemas, dependencies, write modes, and filters | Keep table and stage configuration in `pipeline-config.yaml`. |
| `airflow/dags/` | Schedule and task graph | Keep DAG wiring here; delegate transformations to the `src` package. |
| `docker/<service>/` | Per-service Compose and build configuration | Keep each service's operational files with that service. |
| `src/notebooks/` | Interactive analysis | Keep exploratory notebooks out of production transform modules. |
| `skills/` | Repository-specific coding and structure guidance | Keep skill files grouped by topic. |

## Naming and Import Conventions

- Python packages and modules use lowercase `snake_case`.
- Import project modules from the repository package root, for example `from src.core.registry import ...`.
- Table-specific transformations use descriptive names such as `tickets_transform.py` or `revenue_daily.py`.
- Data-quality modules use `test_<table>.py` and are selected through the registry.
- Stage names and table names are configured in `config/pipeline-config.yaml`; keep registry entries aligned with that configuration.
- Do not add a second source root or relocate `src/` without updating package discovery, Airflow imports, Docker mounts, and launch commands together.

## Navigation and Data Flow

| Task | Starting point |
| --- | --- |
| Run or bootstrap the pipeline | `src/app/run_pipeline.py`, `src/app/run_bootstrap.py` |
| Understand scheduled execution | `airflow/dags/train_pipeline.py` |
| Change pipeline or table settings | `config/pipeline-config.yaml` and `src/core/config/manager/` |
| Add an extractor, transform, or loader | `src/etl/` and `src/core/registry.py` |
| Add a quality rule | `src/data_quality/<stage>/` and the DQ registry |
| Change Spark, MongoDB, or catalog services | `docker/<service>/` and `config/pipeline-config.yaml` |

The runtime flow is:

```text
Airflow DAG -> src.app.run_pipeline -> PipelineOrchestrator
  -> extract -> transform -> optional data quality -> load
```

Airflow imports selected `src` configuration modules directly. Spark submit mounts the whole `src/` package. The Jupyter Compose service mounts `src/notebooks/` at `/app`; moving notebooks requires updating that mount and the README references.

## Configuration, Builds, and Generated Output

- `.env.example` is the template; keep real credentials in ignored `.env.global` and never commit secrets.
- `pyproject.toml` is the dependency and package-discovery source of truth; `uv.lock` records the resolved versions. `requirements.txt` supports pip-based installation.
- The supported Python range is declared in `pyproject.toml`. Use `uv sync --python 3.7` when Python 3.7 is installed, otherwise `uv python install 3.8` followed by `uv sync --python 3.8`.
- The Spark submit image is built from `docker/spark/dockerfile.spark.submit`; it synchronizes dependencies from the project metadata.
- Local environments, Python caches, notebook checkpoints, and package build metadata are generated output and should not be committed.
- Ruff settings live in `pyproject.toml`. Data-quality suites are under `src/data_quality/`; there is no separate top-level unit-test package at present.

## Extension Templates

### Add a Table Transformation

1. Add the table schema, dependencies, filters, and write behavior to `config/pipeline-config.yaml`.
2. Implement the stage-specific transformation under `src/etl/transform/<stage>/`.
3. Register the transformer and extractor/loader as needed in `src/core/registry.py`.
4. Add a corresponding `src/data_quality/<stage>/test_<table>.py` suite and register it.
5. Add the table to the stage's configured table list and confirm the Airflow task ordering/dependencies.

### Add a Source Connector

1. Implement a reader under `src/etl/extract/` using the `BaseExtract` contract.
2. Register the connector in `src/core/registry.py`.
3. Add source configuration through the typed models and config managers.
4. Add or update the service stack under `docker/` only if the connector requires new infrastructure.

## Structure Maintenance

Before moving a package, search for imports and check Docker bind mounts, Airflow task commands, package discovery, notebook paths, and README links. Make structural changes in small steps and validate imports plus the relevant Compose/configuration files before removing old paths.

Update this blueprint whenever a top-level directory, package boundary, runtime entry point, or deployment mount changes. Last updated: 2026-11-02.