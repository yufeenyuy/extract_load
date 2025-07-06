import pandas as pd
import driveextract as driv
import logger as log
import chardet

from io import BytesIO
from io import StringIO
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv

load_dotenv()


drive = driv.authenticate_and_connect_client()

def get_filename_and_files():
    files = driv.list_files(drive, driv.folder_id)
    if type(files) == 'NoneType':
        log.lg.info('No files found in the folder.')
    return files

    
def createdf(nkwa_files = get_filename_and_files(), dfs = dict(), return_text = True):
    if type(nkwa_files) == 'NoneType':
        log.lg.info(f'{type(nkwa_files)}')
        log.lg.info('No files found in the folder. So no data from external provider will be appended.')
        #return dfs
    else:
        for dictio in nkwa_files:
            for k, v in dictio.items():
                if k == 'name' and v.endswith('csv'):
                    fileid = dictio['id']
                    request = driv.authenticate_and_connect_client().files().get_media(fileId = fileid)
                    df = BytesIO()
                    downloader = MediaIoBaseDownload(df, request)

                    while True:
                        done = downloader.next_chunk()
                        if done:
                            break
                    
                    df.seek(0)

                    if return_text:
                        sales = df.read().decode(encoding='utf-8')
                        sales_df = pd.read_csv(StringIO(sales), sep=';', low_memory=False)
                        sales_df.columns = [c.lower() for c in sales_df.columns]
                        dfs[v[:-4]] = sales_df
                        
                    
                if k == 'name' and v.endswith('xlsx'):
                    fileid = dictio['id']
                    request = driv.authenticate_and_connect_client().files().get_media(fileId = fileid)
                    df = BytesIO()
                    downloader = MediaIoBaseDownload(df, request)

                    while True:
                        done = downloader.next_chunk()
                        if done:
                            break
                    
                    df.seek(0)

                    if return_text:
                        prod_df = pd.read_excel(df, engine='openpyxl')
                        prod_df.columns = [c.lower() for c in prod_df.columns]
                        dfs[v[:-5]] = prod_df
                        
    return dfs

