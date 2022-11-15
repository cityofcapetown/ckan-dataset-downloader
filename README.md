# ckan-dataset-downloader
Simple utility for downloading CKAN datasets

## Installation
* Install the requirements `pip3 install -r requirements.txt`

## Usage
Simple use example:
```bash
python3 ckan-dataset-downloader.py --dataset-id "tidy-consumption-data" \
                                   --ckan-api-key <get from your CKAN profile>
```

This will download the contents of the dataset `tidy-consumption-data` to whichever directory you run the script.

Filtered example:
```bash
python3 ckan-dataset-downloader.py --dataset-id "tidy-consumption-data" \
                                   --ckan-api-key <get from your CKAN profile> \
                                   --resource-name-regex "shard 0" \
                                   --destination-dir "/tmp/tidy-consumption-data/"
```

This only selects resources from the dataset `tidy-consumption-data` that contains the string `shard 0`. It also 
downloads to the directory `/tmp/tidy-consumption-data`.