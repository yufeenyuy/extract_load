import os 
import logger as log

from sqlalchemy import create_engine
from sqlalchemy import text



user = os.getenv('POSTGRES_USER')
db_name = os.getenv('POSTGRES_DB')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_PORT')
password = os.getenv('POSTGRES_PASSWORD')

"""Use next two commands to connect to PostgreSQL database and create cursor using psycopg2"""
#conn = ps.connect(host = host, port = port, dbname = db_name, user = user, password = password)
#cur = conn.cursor()

connstr = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}'

engine = create_engine(connstr)

conn = engine.connect()
conn.execute

log.lg.info('Connection to postgres established.')

