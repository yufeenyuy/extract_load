#import psycopg2 as ps
from sqlalchemy import text
import postgres_conn as cur
import datetime as dt
#import pandas as pd
import logger as log
import os

from dotenv import load_dotenv

load_dotenv()


#convert data types of pandas dataframe to strings
def to_str(df):
    df['loaded_at'] = dt.datetime.now()
    df = df.astype(str)
    return df
#Alternative function used to convert data types of a pandas dataframe to strings.
def col2str(df):
    df['loaded_at'] = dt.datetime.now()
    df = df.applymap(str)
    return df

#use a dataframe df2 to update records of another dataframe df1 by a key column.
def updatedf(df1, df2):
    df1.set_index(df1.iloc[:,0], inplace = True)
    df2.set_index(df2.iloc[:,0], inplace = True)
    df1.update(df2)
    return df1

#check whether schema or table exist.
def checkschema_and_table(schemaname,tablename):
    q1 = """select exists(select schema_name from information_schema.schemata where schema_name = :schema)"""
    re= cur.conn.execute(text(q1),{"schema": schemaname})
    result = re.fetchone()[0]
    def checktable(tablename):
        q2 = """select exists(select table_name from information_schema.tables where table_name = :table)"""
        re = cur.conn.execute(text(q2),{"table": tablename})
        result = re.fetchone()[0]
        return result
    return result,checktable(tablename)

#Delete rows from table that are earlier that 5 days
def delete_rows(schemaname, tablename, tablecol) -> None:
    result = checkschema_and_table(schemaname,tablename)
    print(result)
    if result[0] and result[1]:
        q3 = f"""delete from {schemaname}.{tablename} where {tablecol}::date <= current_date - interval '5 days'"""
        with cur.engine.begin() as conn:
            conn.execute(text(q3))
    return None

#This function applies only on datasets that should be ingested once.
def ingest_data(df,schemaname, tablename):
    result = checkschema_and_table(schemaname,tablename)
    with cur.engine.begin() as conn:
        if result[1] is True:
            log.lg.info(f'Schema:{schemaname} and table:{tablename} exist in the database so table:{tablename} will be appended to existing one.')
            df.to_sql(tablename, con=conn,if_exists = 'append',index=False, schema= schemaname,)
            #cur.commit()
        elif result[0] is True and result[1] is False:
            log.lg.info(f'Target Schema:{schemaname} but table:{tablename} does not exist so table:{tablename} will be ingested into Schema:{schemaname}.')
            df.to_sql(tablename, con=conn,index=False, schema= schemaname,)
            log.lg.info(f'{tablename} has been ingested in schema:{schemaname}')
            #cur.commit()
    return






