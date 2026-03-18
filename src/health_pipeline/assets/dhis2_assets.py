"""
Assets to pull data from the demo API end points
"""

import dagster as dg
import io
import pandas as pd
from dagster_duckdb import DuckDBResource

from ..resources import DHIS2Resource

ORG_UNITS_ENDPOINT = "/api/organisationUnits"
ANALYTICS_ENDPOINT = "/api/analytics.csv"


class AnalyticsConfig(dg.Config):
    # TODO yaml or something to store these?
    data_elements: str = "Dkapzovo8Ll;BXgDHhPdFVU;dY4OCwl0Y7Y"
    period: str = "LAST_12_MONTHS"
    org_unit_level: str = "LEVEL-4"


@dg.asset(description="Fetches all organisation units from the DHIS2 API")
def raw_org_units(
    context: dg.AssetExecutionContext,
    dhis2: DHIS2Resource,
    duckdb: DuckDBResource,
) -> None:
    # load org units as json file
    orgunits = dhis2.get_dhis2_json(
        ORG_UNITS_ENDPOINT,
        params={"fields": "id,name,code,parent,level,geometry", "paging": False},
    )
    context.log.info(f"Fetched {len(orgunits['organisationUnits'])} org units")

    # format for duckdb
    df = pd.DataFrame(orgunits["organisationUnits"])
    df["loaded_at"] = pd.Timestamp.now()
    context.log.info("Table ready")

    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.organisation_units 
            AS SELECT * FROM df WHERE 1=0
        """)
        pk_exists = conn.execute("""
                SELECT COUNT(*) FROM duckdb_constraints() 
                WHERE table_name = 'organisation_units' 
                AND constraint_type = 'PRIMARY KEY'
            """).fetchone()[0]

    if not pk_exists:
        conn.execute("ALTER TABLE raw.organisation_units ADD PRIMARY KEY (id)")

        conn.execute("""
            INSERT INTO raw.organisation_units
            SELECT * FROM df
            ON CONFLICT (id) DO NOTHING
        """)

    # TODO should be more accurace, picking rather from raw table
    context.log.info(f"Loaded {len(df)} rows into raw.organisation_units")


@dg.asset(description="Fetches analytics data from the DHIS2 API.")
def raw_analytics(
    context: dg.AssetExecutionContext,
    config: AnalyticsConfig,
    dhis2: DHIS2Resource,
    duckdb: DuckDBResource,
) -> None:
    # fetch dataelement csv
    csv_text = dhis2.get_dhis2_csv(
        ANALYTICS_ENDPOINT,
        params={
            "dimension": [
                f"dx:{config.data_elements}",
                f"pe:{config.period}",
                f"ou:{config.org_unit_level}",
                "co",
            ]
        },
    )
    df = pd.read_csv(io.StringIO(csv_text))
    context.log.info(f"Fetched {len(df)} rows")

    df["loaded_at"] = pd.Timestamp.now()

    # load to db
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.analytics AS 
            SELECT * FROM df WHERE 1=0
        """)
        conn.execute("INSERT INTO raw.analytics SELECT * FROM df")

    context.log.info(f"Loaded {len(df)} rows into raw.analytics")
