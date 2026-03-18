import dagster as dg
from dagster_duckdb import DuckDBResource

from .assets.dhis2_assets import raw_org_units, raw_analytics
from .resources import DHIS2Resource

defs = dg.Definitions(
    assets=[raw_org_units, raw_analytics],
    resources={
        "dhis2": DHIS2Resource(
            base_url=dg.EnvVar("DHIS2_BASE_URL"),
            username=dg.EnvVar("DHIS2_USERNAME"),
            password=dg.EnvVar("DHIS2_PASSWORD"),
        ),
        "duckdb": DuckDBResource(database=dg.EnvVar("DUCKDB_PATH")),
    },
)
