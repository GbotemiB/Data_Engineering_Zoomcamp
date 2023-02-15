from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def fetch(url: str) -> pd.DataFrame:
    """This fetch taxi data from the web and convert it to a dataframe"""
    
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.dtypes) 
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])


    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_to_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """write file to local as csv file"""
    path = Path(f"fhv_data/{dataset_file}.csv.gz")
    df.to_csv(path)
    return path

@task()
def write_gcs(path: Path) -> None:
    """write data to gcs bucket"""
    gcp_block = GcsBucket.load("fhv-bucket")
    gcp_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""

    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    
    data = fetch(dataset_url)
    data = clean(data)
    path = write_to_local(data, dataset_file)
    write_gcs(path)

if __name__=="__main__":
    
    month = 1
    while month <= 12:
        etl_web_to_gcs(year=2019, month=month)
        month += 1
    