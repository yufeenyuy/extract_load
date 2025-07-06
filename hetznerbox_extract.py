import os
import paramiko

from dotenv import load_dotenv

load_dotenv()


target_folder_or_directory = os.getenv('INCOMING')

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

def close_storagbox(ssh,sftp):
    sftp.close()
    ssh.close()




