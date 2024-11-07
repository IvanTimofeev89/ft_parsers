import os

import apiclient.discovery
import httplib2
from dotenv import load_dotenv
from googleapiclient.discovery import Resource
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()


def get_google_connection() -> Resource:
    """Функция для получения подключения к Google таблицам"""

    CREDENTIALS_GOOGLE_FILE = os.getenv("CREDENTIALS_GOOGLE_FILE")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS_GOOGLE_FILE,
        ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    httpAuth = credentials.authorize(httplib2.Http())
    google_service = apiclient.discovery.build("sheets", "v4", http=httpAuth)
    return google_service
