from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
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
    gcp_block = GcsBucket.load("de-gcs")
    gcp_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""

    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    data = clean(df)
    path = write_to_local(data, color, dataset_file)
    write_gcs(path)

if __name__=="__main__":
    etl_web_to_gcs()
    