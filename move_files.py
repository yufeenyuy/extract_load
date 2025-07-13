import os
import pickle
import logger as log
from datetime import datetime, date, timedelta

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


folder_id = '1kZ7u2SuV8gNgJWVgeMxalFz2f3wL_1bs'  

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

df = list_files(authenticate_and_connect_client(),folder_id)

def retrieve_files():
    file_id = []
    get_file = date.today().strftime("%Y%m%d")
    for dic in df:
        if dic['name'] == str(date.today().year):
            year_id = dic['id'].strip()
            print(1)
            year_content = list_files(authenticate_and_connect_client(),year_id)
            for dic in year_content:
                print(2)
                if date.today().day == 1:
                    print(3)
                    prevm = date.today().replace(day=1) - timedelta(days=1)
                    if dic['name'] == f"{prevm.strftime('%B').upper()} {prevm.year}":
                        year_month_id = dic['id']
                        month_content = list_files(authenticate_and_connect_client(),year_month_id)
                        for dic in month_content:
                            if dic['name'] == f"{prevm.strftime('%B')} {str(prevm.day)}":
                                month_day_id = dic['id']
                                day_content = list_files(authenticate_and_connect_client(),month_day_id)
                                for dic in day_content:
                                    if dic['name'] == f'mtn_{get_file}.csv':
                                        file_id.append(dic['id'])
                                    if dic['name'] == f'orange_{get_file}.csv':
                                        file_id.append(dic['id'])
                if date.today().day > 1: 
                    print(4)                       
                    if dic['name'] == f"{date.today().strftime('%B').upper()} {str(date.today().year)}": 
                        year_month_id = dic['id']
                        month_content = list_files(authenticate_and_connect_client(),year_month_id)
                        print(month_content)
                        for dic in month_content:
                            print(5)
                            if dic['name'] == f'{date.today():%B} {date.today().day}':
                                print(6)
                                month_day_id = dic['id']
                                day_content = list_files(authenticate_and_connect_client(),month_day_id)
                                print(day_content)
                                for dic in day_content:
                                    print('check mtn')
                                    if dic['name'] == f'mtn_{get_file}.csv':
                                        file_id.append(dic['id'])
                                    print('check orange')
                                    if dic['name'] == f'orange_{get_file}.csv':
                                        file_id.append(dic['id'])
                        return file_id




file_ids = retrieve_files()

move_to = '1dpm6mM7M7cImoe07vorinEUtI_KqipVw'  #retrieve folder id

def copy_file_to(files_id, new_home_id):
    service = authenticate_and_connect_client()
    for id in files_id:
        copied_file = {
            'parents': [new_home_id]
        }
        service.files().copy(fileId=id, body=copied_file).execute()
    return 

copy_file_to(file_ids, move_to)





