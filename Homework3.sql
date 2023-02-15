-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `pure-album-375307.dezoomcamp2.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://datatalk3/trip data/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(*) as total_fhv_vehicle_record FROM `pure-album-375307.dezoomcamp2.external_yellow_tripdata`;

SELECT affiliated_base_number FROM `pure-album-375307.dezoomcamp2.external_yellow_tripdata` LIMIT 100;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE pure-album-375307.dezoomcamp2.yellow_tripdata_non_partitoned AS
SELECT * FROM pure-album-375307.dezoomcamp2.external_yellow_tripdata;


SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `pure-album-375307.dezoomcamp2.external_yellow_tripdata`; 

SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `pure-album-375307.dezoomcamp2.yellow_tripdata_non_partitoned`;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE pure-album-375307.dezoomcamp2.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM pure-album-375307.dezoomcamp2.yellow_tripdata_non_partitoned;


SELECT DISTINCT(affiliated_base_number) 
FROM pure-album-375307.dezoomcamp2.yellow_tripdata_non_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31' ;

SELECT DISTINCT(affiliated_base_number) 
FROM pure-album-375307.dezoomcamp2.yellow_tripdata_partitoned_clustered 
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
