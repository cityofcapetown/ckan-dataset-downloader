#!/usr/bin/env python3

import argparse
import pathlib
import logging
import re
import time
import urllib.parse

import requests
from tqdm.auto import tqdm

# CKAN CONSTANTS
COCT_CKAN_URL = "https://cctdata.co.za/data-catalogue"
PACKAGE_LOOKUP_PATH = 'api/action/package_show'
NAME_FIELD = 'name'
URL_FIELD = 'url'
ID_FIELD = 'id'
RESOURCES_FIELD = 'resources'

# INTERNAL CONSTANTS
RETRY_DELAY_FACTOR = 10
RETRIES = 3


def _get_dataset_metadata(id_str: str,
                          ckan_api_key: str, ckan_url: str,
                          session: requests.Session) -> dict or None:
    """Utility function wrapping call to get metadata

       :param id_str: Name of CKAN dataset
       :param ckan_api_key: CKAN secret used to authenticate user
       :param ckan_url: hostname of CKAN
       :param session: HTTP session to use to make call
       :return:
       """
    resp = session.get(
        f'{ckan_url}/{PACKAGE_LOOKUP_PATH}',
        params={"id": id_str},
        headers={"X-CKAN-API-Key": ckan_api_key},
    )

    if resp.status_code == 200:
        body = resp.json()['result']

        return body
    elif resp.status_code == 404:
        raise RuntimeError(f"'{id_str}' doesn't exist on {ckan_url}!")
    else:
        logging.warning(f"Got unexpected status code on {id_str} - {resp.status_code}")
        logging.debug(f"response text: {resp.text}")

        return None


def _form_dataset_resources_lookup(dataset_name: str,
                                   ckan_api_key: str, ckan_url: str,
                                   session: requests.Session) -> dict:
    """Utility function for flattening dataset's resources

    :param dataset_name: CKAN dataset name
    :param ckan_api_key: CKAN secret used to authenticate user
    :param ckan_url: CKAN URL
    :return: Dictionary for each resource within a dataset
    """
    dataset_metadata = _get_dataset_metadata(dataset_name,
                                             ckan_api_key, ckan_url, session)

    if dataset_metadata is None:
        raise RuntimeError(f"I don't know what to do with '{dataset_name}' - it doesn't exist")

    dataset_resource_lookup = {
        resource[NAME_FIELD]: resource[URL_FIELD]
        for resource in dataset_metadata[RESOURCES_FIELD]
    }

    return dataset_resource_lookup


def _get_resource_file(url: str, ckan_api_key: str) -> requests.Response:
    last_exception = None
    for t in range(RETRIES):
        try:
            return http_session.get(
                url,
                headers={"X-CKAN-API-Key": ckan_api_key},
            )
        except Exception as e:
            last_exception = e

            sleep_delay = RETRY_DELAY_FACTOR * (t + 1)
            logging.debug(f"Sleeping for {sleep_delay}s...")
            time.sleep(sleep_delay)

    raise last_exception


if __name__ == "__main__":
    # Handling command line arguments
    parser = argparse.ArgumentParser(description='Downloads all the resources in a CKAN dataset to a local dir')
    parser.add_argument('--dataset-id', metavar='dataset_id', type=str,
                        help='ID of CKAN dataset to sync, e.g. "billed-consumption-data"')
    parser.add_argument('--ckan-api-key', metavar='ckan_api_key', type=str, default=None,
                        help='CKAN API Key. If not set, will not be used')
    parser.add_argument('--ckan-url', metavar='ckan_url', type=str, default=COCT_CKAN_URL,
                        help=f'CKAN URL, e.g. {COCT_CKAN_URL}')
    parser.add_argument('--resource-name-regex', metavar='resource_name_regex', type=str, default=None,
                        help="Regex for filtering the resource name")
    parser.add_argument('--destination-dir', metavar='dest_dir', type=str, default=".",
                        help=f'Local directory to sync dataset contents to')
    parser.add_argument('--verbose', action='store_true',
                        help='Turns on verbose logging')
    parser.add_argument('--no-progress-bar', action='store_true',
                        help='Turns off the progress bar')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s"
    )
    logging.debug(f"{args=}")

    # Checking the destination directory exists
    local_dir = pathlib.Path(args.destination_dir)
    assert local_dir.exists(), f"{local_dir} doesn't exist!"

    with requests.Session() as http_session:
        logging.debug(f"Looking up resources for {args.dataset_id}")
        resources_dict = _form_dataset_resources_lookup(args.dataset_id, args.ckan_api_key, args.ckan_url,
                                                        http_session)
        logging.debug(f"{len(resources_dict)=}")

        logging.debug("Assembling list of resources")
        filter_regex = re.compile(args.resource_name_regex) if args.resource_name_regex else None
        resource_list = [
            (resource_name, resource_url)
            for resource_name, resource_url in resources_dict.items()
            if (filter_regex and filter_regex.search(resource_name)) or not filter_regex
        ]
        logging.debug(f"{len(resource_list)=}")
        resource_list = tqdm(resource_list) if not args.no_progress_bar else resource_list

        logging.debug("Iterating over list of responses")
        resource_gen = (
            (
                pathlib.Path(urllib.parse.urlparse(resource_url).path).name,
                _get_resource_file(resource_url, args.ckan_api_key)
            ) for resource_name, resource_url in resource_list
        )
        for filename, http_resp in resource_gen:
            assert http_resp.status_code == 200

            local_path = local_dir / filename
            with open(local_path, "wb") as local_file:
                local_file.write(http_resp.content)
