use reqwest;
use base64::{encode};
use std::collections::HashMap;

struct SAPODataService {
    service_url: String,
    headers: reqwest::header::HeaderMap,
}

impl SAPODataService {
    fn new(service_url: &str, username: &str, password: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Accept", "application/json".parse().unwrap());

        let credentials = format!("{}:{}", username, password);
        let encoded_credentials = encode(credentials);
        let authorization = format!("Basic {}", encoded_credentials);
        headers.insert("Authorization", authorization.parse().unwrap());

        SAPODataService {
            service_url: service_url.to_string(),
            headers,
        }
    }

    fn get_data(&self, endpoint: &str) -> Result<HashMap<String, serde_json::Value>, reqwest::Error> {
        let url = format!("{}/{}", &self.service_url, endpoint);

        let response = reqwest::blocking::Client::new()
            .get(&url)
            .headers(self.headers.clone())
            .send()?
            .json::<HashMap<String, serde_json::Value>>()?;

        Ok(response)
    }
}

fn main() {
    let sap_service = SAPODataService::new("https://your-sap-system-url/odata/service", "your_username", "your_password");
    match sap_service.get_data("entity") {
        Ok(data) => {
            if let Some(values) = data.get("value") {
                println!("{:?}", values);
            }
        }
        Err(e) => println!("Error: {:?}", e),
    }
}
