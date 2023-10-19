import requests
import base64
import logging
import os

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom Exceptions for Better Error Handling
class AuthenticationError(Exception):
    pass

class DataFetchError(Exception):
    pass


class SAP_ODataService:
    def __init__(self, service_url):
        self.service_url = service_url
        self.headers = {
            "Accept": "application/json",
            "Authorization": self.generate_basic_auth_header()
        }

    def generate_basic_auth_header(self):
        username = os.environ.get('SAP_USERNAME')
        password = os.environ.get('SAP_PASSWORD')
        
        if not username or not password:
            raise AuthenticationError("Missing SAP_USERNAME or SAP_PASSWORD environment variables.")

        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"

    def get_data(self, endpoint, params=None):
        url = f"{self.service_url}/{endpoint}"
        response = self.make_request(url, params)
        
        if not response:
            raise DataFetchError(f"Failed to fetch data from {url}")

        data = response.json()
        return self.handle_pagination(data)

    def handle_pagination(self, data):
        while "@odata.nextLink" in data:
            next_page_response = self.make_request(data["@odata.nextLink"])
            
            if not next_page_response:
                raise DataFetchError("Failed to fetch paginated data.")
            
            next_page_data = next_page_response.json()
            data['value'].extend(next_page_data['value'])

            if "@odata.nextLink" in next_page_data:
                data["@odata.nextLink"] = next_page_data["@odata.nextLink"]
            else:
                del data["@odata.nextLink"]

        return data

    def make_request(self, url, params=None):
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            self.handle_errors(response)
            return None

        return response

    def handle_errors(self, response):
        if response.status_code == 400:
            logger.error("Bad Request - The request could not be understood or was missing required parameters.")
        elif response.status_code == 401:
            logger.error("Unauthorized - Authentication failed or user does not have permissions for the requested operation.")
        elif response.status_code == 403:
            logger.error("Forbidden - Authentication succeeded, but the authenticated user does not have access to the requested resource.")
        elif response.status_code == 404:
            logger.error("Not Found - The requested resource could not be found.")
        elif response.status_code == 500:
            logger.error("Internal Server Error.")
        else:
            logger.error(f"HTTP Error {response.status_code}.")
        logger.debug(response.text)


# Usage
sap_service = SAP_ODataService("https://your-sap-system-url/odata/service")
try:
    data = sap_service.get_data("entity")
    if data:
        for item in data['value']:
            print(item)
except (AuthenticationError, DataFetchError) as e:
    logger.error(e)
