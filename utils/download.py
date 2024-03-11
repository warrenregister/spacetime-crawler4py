import requests
import cbor
import time

from utils.response import Response
import requests

# TODO Modify download to work on any website, not just the cache server for UCI websites
def download(url, config, logger=None):
    """
    Download the content of the given URL using the cache server.

    Args:
        url (str): The URL to download.
        config (Config): A Config object containing the crawler configuration.
        logger (logging.Logger, optional): The logger to use for error messages. Defaults to None.

    Returns:
        Response: A Response object containing the downloaded content and metadata.
    """
    host, port = config.cache_server
    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
    try:
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        pass
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
