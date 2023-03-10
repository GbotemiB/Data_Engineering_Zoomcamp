from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect.tasks import task_input_hash
from prefect.filesystems import GitHub




@task(log_prints=True)
def fetch_data(url: str) -> pd.DataFrame:
    """ This Fetch data from url and convert it to dataframe"""
    data = pd.read_parquet(url)
    print("data fetch successfully")
    return data

@task(log_prints=True)
def preprocess(data: pd.DataFrame) -> pd.DataFrame:
    """ This handles and preprocess the data into a usable format"""
    if data.columns[1] == 'lpep_pickup_datetime':
        data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
        data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
    else:
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])
    
    print(f"rows: {len(data)}")
    print("Preprocessing successful")
    return data

@task(log_prints=True)
def save_to_local(data: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """ This will dave data to local"""

    path = Path(f"data/{color}/{dataset_file}.parquet")
    data.to_csv(path, compression="gzip")
    print ("successfully saved to local")
    return path


@task(log_prints=True)
def save_to_gcs(path: Path) -> None:
    """ This will save data to google cloud storage bucket"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("de-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )

    return "Successfully saved to GCS"

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """This is the main function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    data = fetch_data(url=dataset_url)
    data = preprocess(data)
    path = save_to_local(dataset_file=dataset_file, color=color, data=data)
    save_to_gcs(path=path)

    return 

@flow()
def parent_flow(color: str="yellow", months: list[int]=[1,2], year: int=2020) -> None:
    """this is the parent flow for elt web to gcs"""

    for month in months:
        etl_web_to_gcs(color, year, month)
    
    return


if __name__=="__main__":
    Color = "green"
    Year = 2020
    Month = [11]
    parent_flow(color=Color, months=Month, year=Year)