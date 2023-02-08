from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: str) -> Path:
    """download trip data from gcs"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_block = GcsBucket.load("de-gcs")
    gcp_block.get_directory(from_path=gcs_path,local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(retries=3)
def cleaning(path: Path)-> pd.DataFrame:
    """Basic cleaning on the dataset"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count:{df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count:{df['passenger_count'].isna().sum()}")
    return df

@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """write trip data to big query"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    df.to_gbq(
        destination_table="de_taxi.rides",
        project_id="dataeng-375609",
        chunksize=100000,
        if_exists='append',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )

    return

@flow()
def gcs_to_bq() -> None:
    """main flow to load data in Big Query"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color, year, month)
    df = cleaning(path)
    write_to_bq(df)

if __name__=="__main__":
    gcs_to_bq()
