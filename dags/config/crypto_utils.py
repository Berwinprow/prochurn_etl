from cryptography.fernet import Fernet
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


KEY_VAULT_URL = "https://prochurndataengineering.vault.azure.net/"
SECRET_NAME = "DATA-ENCRYPTION-KEY"


def get_fernet():
    """
    Create Fernet object using key from Azure Key Vault
    """
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    secret = client.get_secret(SECRET_NAME)
    return Fernet(secret.value.encode())


def encrypt_value(value, fernet):
    if value is None:
        return None
    return fernet.encrypt(str(value).encode()).decode()


def decrypt_value(value, fernet):
    if value is None:
        return None
    return fernet.decrypt(value.encode()).decode()
