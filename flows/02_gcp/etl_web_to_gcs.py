from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url) -> pd.DataFrame:
    "read taxi data from web / csv into pandas DataFrame"
    #if randint(0,1) > 0:
    #    raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    "fix dtype issues"
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] =  pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns:{df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df:pd.DataFrame,color:str,dataset_file:str) -> Path:
    "write DataFrame out locally as parquet file "
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path,compression='gzip')
    return path

@task()
def write_gcs(path:Path) ->None:
    "upload local paraquet file to gcs"
    gcs_block =  GcsBucket.load("zoomcamp-bucket")
    gcs_block.upload_from_path(from_path=path,to_path=path)
    return

@flow()
def etl_web_to_gcs() -> None:
    "The main ETL function"
    color = "yellow"
    
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"yellow_tripdata_2020-01.csv"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)

if __name__=='__main__':
    etl_web_to_gcs()
