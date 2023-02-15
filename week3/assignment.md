#SQL NOTES to the assignment

CREATE OR REPLACE EXTERNAL TABLE `dataeng-375609.fhv_data.external_fhv_data`
OPTIONS (
  format = "CSV",
  uris = ['gs://fhv-data/fhv_data/*']
);

CREATE OR REPLACE TABLE `dataeng-375609.fhv_data.fhv_2019` as
SELECT * from `dataeng-375609.fhv_data.external_fhv_data`;

SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dataeng-375609.fhv_data.external_fhv_data`;

SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dataeng-375609.fhv_data.fhv_2019`;

SELECT COUNT(*) 
FROM `dataeng-375609.fhv_data.fhv_2019` AS FHV
WHERE FHV.DOlocationID IS NULL AND FHV.PUlocationID IS NULL;

CREATE OR REPLACE TABLE `dataeng-375609.fhv_data.fhv_2019_partioned_cluster`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `dataeng-375609.fhv_data.fhv_2019`;

SELECT COUNT(DISTINCT(affiliated_base_number))
FROM `dataeng-375609.fhv_data.fhv_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT COUNT(DISTINCT(affiliated_base_number))
FROM `dataeng-375609.fhv_data.fhv_2019_partioned_cluster`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';