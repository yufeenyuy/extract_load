import os
import logger as log
import pandas as pd
import hetznerbox_extract as hz 

from io import BytesIO

# Function to initiates connection to the Hetzner storage box and extract excel workbook containing data for processing.
# It returns a dictionary containing DataFrames for each sheet in the workbook.
# The dictionary keys are the sheet names and the values are the corresponding DataFrames.
def extract_to_df(dfs = dict()):
    ssh, sftp = hz.connect_sftp()
    wkbook = sftp.listdir(hz.target_folder_or_directory)
    remote_filepath = os.path.join(hz.target_folder_or_directory,wkbook[0])
    with sftp.open(remote_filepath, 'rb') as remote_file:
        file_bytes = remote_file.read()
        excel_file = BytesIO(file_bytes)
        with pd.ExcelFile(excel_file) as xls:
            for sheet_name in xls.sheet_names:
                dfs[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
    return dfs



