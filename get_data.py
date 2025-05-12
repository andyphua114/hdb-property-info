from utils import check_response, get_latlon, get_token
import pandas as pd
import requests
import time


def get_data():
    """Retrive the HDB property information data and metadata from data.gov.sg.

    Returns:
        pd.DataFrame: A DataFrame containing the HDB property information,
        with a mapped `bldg_contract_town` to the full text as a new column `Area`
    """

    # retrive the dataset and store as DataFrame
    dataset_id = "d_17f5382f26140b1fdae0ba2ef6239d2f"

    # initiate download
    s = requests.Session()

    initiate_download_response = s.get(
        f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download",
        headers={"Content-Type": "application/json"},
        json={},
    )

    print(initiate_download_response.json()["data"]["message"])

    # poll download
    MAX_POLLS = 5
    for i in range(MAX_POLLS):
        poll_download_response = s.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download",
            headers={"Content-Type": "application/json"},
            json={},
        )
        if "url" in poll_download_response.json()["data"]:
            DOWNLOAD_URL = poll_download_response.json()["data"]["url"]
            df = pd.read_csv(DOWNLOAD_URL)

            print("\nDataframe loaded!")
            break

        if i == MAX_POLLS - 1:
            print(
                f"{i+1}/{MAX_POLLS}: No result found, possible error with dataset, please try again or let us know at https://go.gov.sg/datagov-supportform\n"
            )
        else:
            print(f"{i+1}/{MAX_POLLS}: No result yet, continuing to poll\n")
            time.sleep(3)

    print(len(df))

    # filter to keep only residential
    df = df[df["residential"] == "Y"]

    print(len(df))

    # retrieve the metadata of dataset and convert string of `bldg_contract_town` description text into a mapping DataFrame
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

    token = get_token()

    start_time = time.time()

    # get lat lon of address
    df[["lat", "lon"]] = (
        df["address"].apply(lambda x: get_latlon(x, token=token)).apply(pd.Series)
    )

    print(
        "Time taken for lat-lon retrieval: {} mins".format(
            (time.time() - start_time) / 60
        )
    )

    df.to_csv("data/hdb-property-info.csv", index=False)


if __name__ == "__main__":
    get_data()
