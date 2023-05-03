import os
import logging
from hashlib import sha256
from urllib.parse import urlparse

def get_logger(name, filename=None):
    """
    Create a logger with the specified name and optional filename.
    
    Args:
        name (str): The name of the logger.
        filename (str, optional): The filename for the log file. Defaults to None.

    Returns:
        logger (logging.Logger): A configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    """
    Generate a hash of the given URL.
    
    Args:
        url (str): The URL to be hashed.

    Returns:
        str: A SHA-256 hash of the URL.
    """

    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}".encode("utf-8")).hexdigest()

def normalize(url):
    """
    Normalize the given URL by removing a trailing forward slash, if present.

    Args:
        url (str): The URL to be normalized.

    Returns:
        str: The normalized URL.
    """
    if url.endswith("/"):
        return url.rstrip("/")
    return url
