
set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/${TAXI_TYPE}"
#    URL_PREFIX="https://s3.amazonaws.com/nyc-tlc/trip+data"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  echo "compressing ${LOCAL_PATH}"
  gzip ${LOCAL_PATH}
done
