import requests
import base64

class SAP_ODataService:
    def __init__(self, service_url, username, password):
        self.service_url = service_url
        self.headers = {
            "Accept": "application/json",
            "Authorization": self.generate_basic_auth_header(username, password)
        }
    
    def generate_basic_auth_header(self, username, password):
        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"

    def get_data(self, endpoint, params=None):
        url = f"{self.service_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        # Error Handling
        if response.status_code != 200:
            self.handle_errors(response)
            return None

        data = response.json()

        # Handle Pagination (OData often uses the '@odata.nextLink' property for pagination)
        while "@odata.nextLink" in data:
            next_page_response = requests.get(data["@odata.nextLink"], headers=self.headers)
            
            if next_page_response.status_code != 200:
                self.handle_errors(next_page_response)
                return None
            
            next_page_data = next_page_response.json()
            data['value'].extend(next_page_data['value'])
            
            if "@odata.nextLink" in next_page_data:
                data["@odata.nextLink"] = next_page_data["@odata.nextLink"]
            else:
                del data["@odata.nextLink"]
        
        return data

    def handle_errors(self, response):
        if response.status_code == 400:
            print("Bad Request - The request could not be understood or was missing required parameters.")
        elif response.status_code == 401:
            print("Unauthorized - Authentication failed or user does not have permissions for the requested operation.")
        elif response.status_code == 403:
            print("Forbidden - Authentication succeeded, but the authenticated user does not have access to the requested resource.")
        elif response.status_code == 404:
            print("Not Found - The requested resource could not be found.")
        elif response.status_code == 500:
            print("Internal Server Error.")
        else:
            print(f"HTTP Error {response.status_code}.")

        print(response.text)


# Usage
sap_service = SAP_ODataService("https://your-sap-system-url/odata/service", "your_username", "your_password")
data = sap_service.get_data("entity")
if data:
    for item in data['value']:
        print(item)
