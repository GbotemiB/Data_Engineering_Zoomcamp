from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True)#, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_data_from_gcs(color: str, year: int, month: int) -> Path:
    """ This functions fetches data from gcs"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcp_block = GcsBucket.load("de-gcs")
    gcp_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(gcs_path)

@task()
def load_data(path: Path) -> pd.DataFrame:
    """ This function load the file in the gcs path and converts it to a parquet file"""

    data = pd.read_parquet(path)
    return data

@task(log_prints=True, retries=3)
def write_to_bq(data: pd.DataFrame) -> None:
    """ This functions writes data to big query"""
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
    
    data.to_gbq(
        destination_table="de_taxi.rides",
        project_id="dataeng-375609",
        chunksize=100000,
        if_exists='append',
        progress_bar=True,
        credentials=gcp_credentials_block.get_credentials_from_service_account()
    )
    print ("data written to big query successfully")
    return len(data)


@flow()
def gcs_to_bq(color: str, year: int, month: int) -> int:

    data = get_data_from_gcs(color, year, month)
    data = load_data(data)
    len_of_row = write_to_bq(data)
    return len_of_row

@flow()
def parent_gcs_to_bq(color: str="Green", months: list[int]=[1,2], year: int=2021) -> None:
    """parent flow for gcs to BQ """
    rows_processed = []
    for month in months:
        rows = gcs_to_bq(color=color, month=month, year=year)
        rows_processed.append(rows)
    print(f"rows processed: {rows_processed}")



if __name__=="__main__":
    color = "yellow"
    months = [2,3]
    year = 2020
    parent_gcs_to_bq(color=color, year=year, months=months)

"""

main flow should print total number of rows
Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

load yellow taxi data for feb and march 2019 in parquest format
How many rows did your flow code process?

"""