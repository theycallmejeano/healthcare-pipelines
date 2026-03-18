## Healthcare Pipelines

# health-pipeline

This repo contains code for a DHIS2 → DuckDB data pipeline as follows:

- **API connection**, to extract data from the DHIS2 Demo API
- **Data loading**, to store raw data into DuckDB
- **Transformations**, to clean and model data using dbt

## Setting Up

1. Set up the poetry environment and install packages
```bash
poetry shell
poetry install
```

2. Make a copy of `.env.example` and name it `.env`. Populate with your DHIS2 credentials — the defaults point to the public demo instance and work out of the box.

## Running the Pipeline

Load the environment and start the Dagster UI:
```bash
dagster dev -m health_pipeline.definitions
```

Open `http://localhost:3000` and materialise assets from the UI.