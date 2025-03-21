from datetime import datetime, timedelta
from threading import Lock
from azure.identity import AzureCliCredential

AZURE_TOKEN_RESOURCE = "https://ossrdbms-aad.database.windows.net"

class AzureTokenCache:
    def __init__(self):
        self._lock = Lock()
        self._token = None
        self._expiration = None
        self._credential = AzureCliCredential()

    def get_token(self) -> str:
        with self._lock:
            now = datetime.utcnow()
            if not self._token or not self._expiration or (self._expiration - now) < timedelta(minutes=5):
                token_response = self._credential.get_token(AZURE_TOKEN_RESOURCE)
                self._token = token_response.token
                self._expiration = now + timedelta(minutes=55)
            return self._token

# Global token cache instance
token_cache = AzureTokenCache()
