import os

CURRENT_DIR = os.path.dirname(__file__).split("/")[-1]
CONFIGPATH = os.path.join(CURRENT_DIR, "config")
CREDSPATH = os.path.join(CONFIGPATH, "credentials")
CLIENT_SECRETS_PATH = os.path.join(CREDSPATH, "owid-analytics-client-secrets.json")
