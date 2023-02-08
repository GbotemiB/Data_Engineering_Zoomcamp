from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect.tasks import task_input_hash

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """This fetch taxi data from the web and convert it to a dataframe"""
    
    df = pd.read_csv(url)
    return df

@task(retries=3, log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_to_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write file to local as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """write data to gcs bucket"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("dataeng-bucket")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(month: int, year: str, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    data = clean(df)
    path = write_to_local(data, color, dataset_file)
    write_gcs(path)

@flow()
def parent_etl_flow(months: list[int]=[1,2], year: int=2021, color: str = "yellow") -> None:
    """parent flow for web to gcs"""
    for month in months:
        etl_web_to_gcs(month, year, color)

    return

if __name__=="__main__":
    months = [1,2,3]
    year = 2021
    color = "yellow"
    parent_etl_flow(months, year, color)
    