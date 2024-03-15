import requests

from utils.response import Response
import requests


def download(url, config, logger=None):
    """
    Download the content of the given URL from the internet.

    Args:
        url (str): The URL to download.
        config (Config): A Config object containing the crawler configuration.
        logger (logging.Logger, optional): The logger to use for error messages. Defaults to None.

    Returns:
        Response: A Response object containing the downloaded content and metadata.
    """

    headers = {
        "User-Agent": config.user_agent
    }

    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()  # Raise an exception for non-2xx status codes

        if resp.content:
            return Response({"content": resp.content, "url": url, "status": resp.status_code})

    except requests.exceptions.RequestException as e:
        if logger:
            logger.error(f"Error downloading {url}: {e}")

        return Response({
            "error": str(e),
            "status": resp.status_code if hasattr(resp, "status_code") else None,
            "url": url
        })

    if logger:
        logger.error(f"Spacetime Response error {resp} with url {url}.")

    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code if hasattr(resp, "status_code") else None,
        "url": url
    })