from google_auth_oauthlib.flow import InstalledAppFlow
from site_analytics import CLIENT_SECRETS_PATH


def google_analytics_authenticate():
    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_PATH,
        scopes=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/analytics.readonly",
        ],
    )
    flow.run_local_server()
    credentials = flow.credentials
    return credentials
