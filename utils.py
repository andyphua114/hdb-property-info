from dotenv import load_dotenv
import os
import pandas as pd
import requests


def check_response(response):
    """Check the response based on status code.

    Args:
        response (requests.Response): The HTTP response returned by `requests.get()` or `requests.post()`.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        requests.HTTPError: If the status code is not 200
    """

    if response.status_code == 200:
        return response.json()
    else:
        raise requests.HTTPError(
            f"Request failed with status code {response.status_code}: {response.text}"
        )


def get_token():
    """Retrieve your OneMap API token. The email and password must be defined in a .env file.

    Returns:
        string: OneMap API token
    """

    # Load environment variables from .env file
    load_dotenv(override=True)

    url = "https://www.onemap.gov.sg/api/auth/post/getToken"

    payload = {
        "email": os.environ["ONEMAP_EMAIL"],
        "password": os.environ["ONEMAP_EMAIL_PASSWORD"],
    }

    response = requests.request("POST", url, json=payload)
    token = check_response(response)["access_token"]

    return token


def get_latlon(location):
    """Retrieve your OneMap API token. The email and password must be defined in a .env file.

    Args:
        location (string): The search term to be used to query the OneMamp Search API.

    Returns:
        tuple: latitude and longitude of location
    """

    token = get_token()
    url = "https://www.onemap.gov.sg/api/common/elastic/search?searchVal={}&returnGeom=Y&getAddrDetails=Y&pageNum=1".format(
        location
    )
    headers = {"Authorization": "Bearer {}".format(token)}

    response = requests.get(url, headers=headers)
    search_result = check_response(response)

    if search_result["found"] == 0:
        return (None, None)
    else:
        # just take the first result as OneMap API results sorted based on estimated relevance
        return (
            search_result["results"][0]["LATITUDE"],
            search_result["results"][0]["LONGITUDE"],
        )


def get_data():
    """Retrive the HDB property information data and metadata from data.gov.sg.

    Returns:
        pd.DataFrame: A DataFrame containing the HDB property information,
        with a mapped `bldg_contract_town` to the full text as a new column `Area`
    """

    # retrive the dataset and store as DataFrame
    dataset_id = "d_17f5382f26140b1fdae0ba2ef6239d2f"
    url = "https://data.gov.sg/api/action/datastore_search?resource_id=" + dataset_id

    response = requests.get(url)

    df = pd.DataFrame(check_response(response)["result"]["records"])

    # filter to keep only residential
    df = df[df["residential"] == "Y"]

    # retrive the metadata of dataset and convert string of `bldg_contract_town` description text into a mapping DataFrame
    response = requests.get(
        "https://api-production.data.gov.sg/v2/public/api/datasets/{}/metadata".format(
            dataset_id
        ),
        headers={"Accept": "*/*"},
    )
    metadata = check_response(response)
    metadata_mapping = metadata["data"]["columnMetadata"]["map"]
    town_value = "bldg_contract_town"
    town_key = next((k for k, v in metadata_mapping.items() if v == town_value), None)

    town_description = metadata["data"]["columnMetadata"]["metaMapping"][town_key][
        "description"
    ]

    town_pairs = []
    towns = town_description.split(" - ")
    for idx, t in enumerate(towns[:-1]):
        town_pairs.append((t.rsplit(" ", 1)[1], towns[idx + 1].rsplit(" ", 1)[0]))

    town_df = pd.DataFrame(town_pairs, columns=["bldg_contract_town", "Area"])

    df = pd.merge(df, town_df, how="left", on="bldg_contract_town")
    df["address"] = df["blk_no"] + " " + df["street"]

    df[["lat", "lon"]] = df["address"].apply(lambda x: get_latlon(x)).apply(pd.Series)

    return df
