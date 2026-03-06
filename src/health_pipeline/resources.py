import requests
import dagster as dg


class DHIS2Resource(dg.ConfigurableResource):
    base_url: str
    username: str
    password: str

    def _get_dhis2(
        self, endpoint: str, params: dict = {}, timeout: int = 30
    ) -> requests.Response:
        response = requests.get(
            f"{self.base_url}{endpoint}",
            auth=(self.username, self.password),
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response

    def get_dhis2_json(self, endpoint: str, params: dict = {}) -> dict:
        return self._get_dhis2(endpoint, params).json()

    def get_dhis2_csv(self, endpoint: str, params: dict = {}) -> str:
        return self._get_dhis2(endpoint, params, timeout=60).text
