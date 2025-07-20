import os
import paramiko

from dotenv import load_dotenv

load_dotenv()

# The target folder or directory where files will be read from.
target_folder_or_directory = os.getenv('INCOMING')

# Function to connect to the Hetzner Storage Box using SFTP.
# It returns an SSH client and an SFTP client.
def connect_sftp():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(
        hostname = os.getenv('HETZNER_HOST'),
        username = os.getenv('HETZNER_USERNAME'),
        password = os.getenv('HETZNER_PASSWORD'),
        port=22,
        look_for_keys=False,
        allow_agent=False
    )
    return ssh, ssh.open_sftp()

# function to close the SSH and SFTP connections.
# It takes an SSH client and an SFTP client as parameters.
def close_storagbox(ssh,sftp):
    sftp.close()
    ssh.close()




