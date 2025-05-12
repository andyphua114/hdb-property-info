from dotenv import load_dotenv
import os
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


def get_latlon(location, token):
    """Retrieve your OneMap API token. The email and password must be defined in a .env file.

    Args:
        location (string): The search term to be used to query the OneMamp Search API.

    Returns:
        tuple: latitude and longitude of location
    """

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
