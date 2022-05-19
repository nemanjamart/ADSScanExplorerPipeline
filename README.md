# ADSScanExplorerPipeline
## Logic
The pipeline loops through the input folder structure identifying journal volumes and compare the file status to the ingestion db to detect any updates. The input folder should contain3 subfolders
* bitmaps -- images
* lists -- metadata
* ocr -- ocr files

#TODO Write more of file strucutre
## Setup

* The pipeline needs at at minimum a DB to run the baseline ingestion pipeline. 
* An ElasticSearch instance is needed to index the associated OCR files
* A S3 Bucket is needed to upload the actual image files

### Pipeline

Start with setting up the pipeline container. Make sure to set the input folder (with all image files, top files and ocr files) under volumes in the docker-compose.yaml. This will mount the folder into the container making it accessible to run the pipeline. 
```
docker compose -f docker-compose.yaml up -d
```
This will start a Celery instance. If running on a dev environment you could be running without a RabbitMQ backend with setting CELERY_ALWAYS_EAGER=True in config.py


### Elastic Search

Setup up the Elastic Search docker container

```
docker compose -f docker-compose_es.yaml up -d
```

Setup the index by running through the pipeline container:

```
docker exec -it ads_scan_explorer_pipeline python setup_es.py 
```

### Database
Setup a postgresql container
```
docker compose -f docker-compose_es.yaml up -d
```

Prepare the database:

```
docker exec -it postgres bash -c "psql -c \"CREATE ROLE scan_explorer WITH LOGIN PASSWORD 'scan_explorer';\""
docker exec -it postgres bash -c "psql -c \"CREATE DATABASE scan_explorer_pipeline;\""
docker exec -it postgres bash -c "psql -c \"GRANT CREATE ON DATABASE scan_explorer_pipeline TO scan_explorer;\""
```

Setup the tables by running through the pipeline container:
```
docker exec -it ads_scan_explorer_pipeline python setup_db_.py 
```

## Usage
The pipeline can be run in a couple of different setups. Just dry run and check which volumes in the input folder that are new or have updated files. The volumes will be added to the db unde the table "journal_volume"
```
docker exec -it ads_scan_explorer_pipeline python run.py --input-folder=/opt/ADS_scans_sample/ NEW --process=False
```

Process all volumes with new or update status, updating the db and ocr index but leaving the heaveir task of uploading all image files to the S3 Bucket
```
docker exec -it ads_scan_explorer_pipeline python run.py --input-folder=/opt/ADS_scans_sample/ --upload-files=n --index-ocr=y NEW --process=True
```

Process a single or multiple volumes by id. Will be processed/reprocessed disregarding previous status. Id is either volume id (uuid) or journal + volume. Multiple ids can be input comma separated
```
docker exec -it ads_scan_explorer_pipeline python run.py --input-folder=/opt/ADS_scans_sample/ --upload-files=y --index-ocr=y SINGLE --id=phae.17500,c949f56b-cef6-43ea-b34c-cf5cc1bcdd41
```