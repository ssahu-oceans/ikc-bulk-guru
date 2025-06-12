"""
IBM Cloud Pak for Data (CPD) client for API connections
"""

import requests
import urllib3
import os
from dotenv import load_dotenv

urllib3.disable_warnings()


class CPDClient:
    """IBM Cloud Pak for Data API client with session management and authentication"""
    
    def __init__(self, config_file=None):
        """
        Initialize CPD client with environment variables or config file
        
        Args:
            config_file (str): Optional path to .env file
        """
        if config_file:
            load_dotenv(config_file)
        else:
            load_dotenv()
            
        # Load configuration
        self.cpd_host = os.environ.get('CPD_HOST')
        self.api_key = os.environ.get('API_KEY')
        self.username = os.environ.get('USERNAME')
        self.password = os.environ.get('PASSWORD')
        self.env_type = os.environ.get('ENV_TYPE', 'SW')
        self.auth_type = os.environ.get('AUTH_TYPE', 'PASSWORD')
        
        # Validate required config
        self._validate_config()
        
        # Initialize session and headers
        self.session = requests.Session()
        self.headers = {'Content-Type': "application/json"}
        self._authenticated = False
    
    def _validate_config(self):
        """Validate required environment variables"""
        if not self.cpd_host:
            raise ValueError("CPD_HOST environment variable is required")
        
        if self.env_type == "SAAS" and not self.api_key:
            raise ValueError("API_KEY required for SAAS environment")
        elif self.env_type != "SAAS":
            if self.auth_type == "PASSWORD" and not (self.username and self.password):
                raise ValueError("USERNAME and PASSWORD required for password authentication")
            elif self.auth_type != "PASSWORD" and not (self.username and self.api_key):
                raise ValueError("USERNAME and API_KEY required for API key authentication")
    
    def authenticate(self):
        """Generate and set bearer token from CPD credentials"""
        if self.env_type == "SAAS":
            url = f"https://iam.cloud.ibm.com/identity/token?grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={self.api_key}"
            try:
                response = self.session.post(url, verify=False)
            except requests.RequestException as e:
                raise ConnectionError(f"Error authenticating to IBM Cloud: {str(e)}")
        else:
            url = f"https://{self.cpd_host}/icp4d-api/v1/authorize"
            payload = (
                {"username": self.username, "password": self.password} 
                if self.auth_type == "PASSWORD" 
                else {"username": self.username, "api_key": self.api_key}
            )
            try:
                response = self.session.post(url, json=payload, verify=False)
            except requests.RequestException as e:
                raise ConnectionError(f"Error authenticating to CPD: {str(e)}")

        if response.status_code == 200:
            token_key = 'access_token' if self.env_type == "SAAS" else 'token'
            access_token = response.json()[token_key]
            self.headers['Authorization'] = f"Bearer {access_token}"
            self._authenticated = True
        else:
            raise ConnectionError(
                f"Authentication failed.\n"
                f"Status: {response.status_code}\n"
                f"Response: {response.text}"
            )
    
    def _ensure_authenticated(self):
        """Ensure client is authenticated before making requests"""
        if not self._authenticated:
            self.authenticate()
    
    def get(self, endpoint, params=None, **kwargs):
        """Make authenticated GET request"""
        self._ensure_authenticated()
        url = f"https://{self.cpd_host}{endpoint}"
        return self.session.get(url, headers=self.headers, params=params, verify=False, **kwargs)
    
    def post(self, endpoint, json=None, data=None, **kwargs):
        """Make authenticated POST request"""
        self._ensure_authenticated()
        url = f"https://{self.cpd_host}{endpoint}"
        return self.session.post(url, headers=self.headers, json=json, data=data, verify=False, **kwargs)
    
    def put(self, endpoint, json=None, data=None, **kwargs):
        """Make authenticated PUT request"""
        self._ensure_authenticated()
        url = f"https://{self.cpd_host}{endpoint}"
        return self.session.put(url, headers=self.headers, json=json, data=data, verify=False, **kwargs)

    def patch(self, endpoint, json=None, data=None, **kwargs):
        """Make authenticated PATCH request"""
        self._ensure_authenticated()
        url = f"https://{self.cpd_host}{endpoint}"
        return self.session.patch(url, headers=self.headers, json=json, data=data, verify=False, **kwargs)
        
    def delete(self, endpoint, **kwargs):
        """Make authenticated DELETE request"""
        self._ensure_authenticated()
        url = f"https://{self.cpd_host}{endpoint}"
        return self.session.delete(url, headers=self.headers, verify=False, **kwargs)
    
    def search(self, query_payload, auth_scope="category"):
        """
        Perform search using CPD search API
        
        Args:
            query_payload (dict): Elasticsearch-style query
            auth_scope (str): Authorization scope
            
        Returns:
            requests.Response: API response
        """
        endpoint = f"/v3/search?auth_scope={auth_scope}"
        return self.post(endpoint, json=query_payload)
    
    def close(self):
        """Close the session"""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Convenience function
def create_client(config_file=None):
    """Create and authenticate a CPD client"""
    client = CPDClient(config_file)
    client.authenticate()
    return client