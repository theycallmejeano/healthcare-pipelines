import dagster as dg
import io
import pandas as pd

from ..resources import DHIS2Resource

ORG_UNITS_ENDPOINT = "/api/organisationUnits"
ANALYTICS_ENDPOINT = "/api/analytics.csv"


class AnalyticsConfig(dg.Config):
    data_elements: str = "Dkapzovo8Ll;BXgDHhPdFVU;dY4OCwl0Y7Y"
    period: str = "LAST_12_MONTHS"
    org_unit_level: str = "LEVEL-4"


@dg.asset(description="Fetches all organisation units from the DHIS2 API")
def raw_org_units(context: dg.AssetExecutionContext, dhis2: DHIS2Resource) -> dict:
    data = dhis2.get_dhis2_json(ORG_UNITS_ENDPOINT, params={"paging": False})
    context.log.info(f"Fetched {len(data['organisationUnits'])} org units")
    return data


@dg.asset(description="Fetches analytics data from the DHIS2 API.")
def raw_analytics(
    context: dg.AssetExecutionContext,
    config: AnalyticsConfig,
    dhis2: DHIS2Resource,
) -> pd.DataFrame:
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
    return df
