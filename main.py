import logger as log
import postgresops as ps
import yelp_restaurants_extract as yelp
import driveextract as d
import convert_driveextract_to_df as dr
import convert_hetzner_box_extract_to_df as hzr


from dotenv import load_dotenv

load_dotenv()


def ingest_drive_production_dt():
    dfs = dr.createdf()
    production = ps.to_str(dfs['production'])
    log.lg.info('Ingesting production in Postgres...')
    ps.ingest_data(production, 'raw_datasets', 'production')
    log.lg.info('production ingested successfully.')
    return None

def ingest_drive_sales_dt():
    dfs = dr.createdf()
    sales = ps.to_str(dfs['sales'])
    log.lg.info('Ingesting sales into Postgres...')
    ps.ingest_data(sales, 'raw_datasets', 'sales')
    log.lg.info('sales ingested successfully.')
    return None

def ingest_hz_production_dt():
    dfs = hzr.extract_to_df()
    production = ps.to_str(dfs['Production_Data'])
    log.lg.info('Ingesting production_data into Postgres...')
    ps.ingest_data(production, 'raw_datasets', 'production_data')
    log.lg.info('production_data ingested successfully.')
    return None

def ingest_hz_sales_dt():
    dfs = hzr.extract_to_df()
    sales = ps.to_str(dfs['Sales_Data'])
    log.lg.info('Ingesting sales_data into Postgres...')
    ps.ingest_data(sales, 'raw_datasets', 'sales_data')
    log.lg.info('sales_data ingested successfully.')
    return None

def ingest_yelp_business():
    businesses_df = ps.to_str(yelp.get_business_reviews()[0])
    log.lg.info('Ingesting yelp businesses data into Postgres...')
    ps.ingest_data(businesses_df, 'raw_datasets', 'yelp_businesses')
    log.lg.info('yelp businesses ingested successfully.')

def ingest_yelp_business_reviews():
    business_reviews_df = ps.to_str(yelp.get_business_reviews()[1])
    log.lg.info('Ingesting yelp business reviews data into Postgres...')
    ps.ingest_data(business_reviews_df, 'raw_datasets', 'yelp_business_reviews')
    log.lg.info('yelp business reviews ingested successfully.')

def data_ingest():
    ingest_drive_production_dt()
    ingest_drive_sales_dt()
    ingest_hz_production_dt()
    ingest_hz_sales_dt()
    ingest_yelp_business()
    ingest_yelp_business_reviews()
    return None

if __name__ == '__main__':
    log.lg.info("Start Data Ingestion process")
    data_ingest()
    log.lg.info("Data Ingestion process successfully completed.")
    d.move_file_to_folder(service=d.authenticate_and_connect_client(), 
                    items=d.list_files(service=d.authenticate_and_connect_client(), 
                                    folder_id=d.folder_id), 
                  new_folder_id=d.move_to)
    log.lg.info('Ingested files have been moved to archive folder.')

