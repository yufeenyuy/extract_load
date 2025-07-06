import os
import pickle
import logger as log


from google_auth_oauthlib.flow import InstalledAppFlow
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# Set environment variable to allow insecure transport for OAuth
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

from dotenv import load_dotenv

# http://localhost:3000/api/auth/callback

load_dotenv()

scope = ['https://www.googleapis.com/auth/drive']

CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
        "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_X500_CERT_URL"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRETE"),
        "redirect_url": [os.getenv("GOOGLE_REDIRECT_URI")]
    }
}

flow = InstalledAppFlow.from_client_config(
    client_config=CLIENT_CONFIG,
    scopes=scope)

flow.redirect_uri = CLIENT_CONFIG["web"]["redirect_url"][0]
log.lg.info(f"Redirect URI:, {flow.redirect_uri}")


auth_url, state = flow.authorization_url(
    access_type='offline',
    include_granted_scopes='true',
    prompt='consent'
)

def authenticate_and_connect_client():
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            creds = flow.run_local_server(port=8080)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials= creds)
    return service


folder_id = os.getenv("SOURCE")

def list_files(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q = query, 
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        fields="files(id, name, mimeType, modifiedTime, createdTime)").execute()
    items =results.get('files',[])

    if type(items) == 'NoneType':
        log.lg.info('No files found in retrieve folder')
    else:
        return items

move_to = os.getenv("ARCHIVE")   
def move_file_to_folder(service, items, new_folder_id):
    id_list = [item['id'] for item in items]
    if len(id_list) == 0:
        log.lg.info('No files to move')
        return
    for id in id_list:
        file = service.files().get(fileId=id, fields='parents', supportsAllDrives=True).execute()
        previous_parents = ",".join(file.get('parents', []))

        file = service.files().update(
            fileId=id,
            addParents=new_folder_id,
            removeParents=previous_parents,
            supportsAllDrives=True,
            fields='id, parents'
        ).execute()
    return


