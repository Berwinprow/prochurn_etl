from cryptography.fernet import Fernet
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

KEY_VAULT_URL = "https://prochurndataengineering.vault.azure.net/"
SECRET_NAME = "DATA-ENCRYPTION-KEY"

# ------------------------------------------------------------------
# 🔑 Fernet Initialization
# ------------------------------------------------------------------

def get_fernet():

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = KEY_VAULT_URL, credential = credential)
    secret = client.get_secret(SECRET_NAME)

    # Create Fernet object using the secret key
    return Fernet(secret.value.encode())

# ------------------------------------------------------------------
# 🔒 Encryption Helper
# ------------------------------------------------------------------

def encrypt_value(value,fernet):
    if value is None:
        return None
    # Convert value to string, then bytes, encrypt, return string
    return fernet.encrypt(str(value).encode()).decode()

# ------------------------------------------------------------------
# 🔓 Decryption Helper
# ------------------------------------------------------------------

def decrypt_value(value,fernet):
    if value is None:
        return None
    return fernet.decrypt(value.encode()).decode()
    