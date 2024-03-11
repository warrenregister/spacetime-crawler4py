from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
from urllib.parse import urlparse, parse_qs
from difflib import SequenceMatcher



# TODO: Remove simhashing and either modify or remove url similarity checks
class Worker(Thread):
    """
    The Worker class is a subclass of Thread that downloads URLs from the Frontier
    and processes them using the scraper function from scraper.py. Each Worker
    instance runs concurrently in a separate thread.
    
    Attributes:
        config (Config): A Config object containing the crawler configuration.
        frontier (Frontier): A Frontier object managing the list of URLs to be downloaded and their state.
    """

    def __init__(self, worker_id, config, frontier):
        """
        Initialize the Worker with the given worker_id, configuration, and frontier.
        
        Args:
            worker_id (int): A unique identifier for the worker.
            config (Config): A Config object containing the crawler configuration.
            frontier (Frontier): A Frontier object managing the list of URLs to be downloaded and their state.
        """
        self.logger = get_logger(f"Worker-{worker_id}", "WORKER")
        self.config = config
        self.frontier = frontier
        self.similarity_threshold = 0.95
        self.max_depth = 28
        self.min_words = 30
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    def run(self):
        """
        Run the worker loop until there are no more URLs to be downloaded.
        """
        while True:
            tbd_url, depth = self.frontier.get_tbd_url()
            if tbd_url is None:
                self.logger.info("Frontier is empty. Stopping.")
                break
                
            if depth > self.max_depth:
                self.logger.info(f"Skipping {tbd_url}, reached max depth.")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            # check if url is similar to low data urls
            low_data, error = self.frontier.get_bad_urls()
            if is_similar_url(tbd_url, low_data):
                self.logger.info(f"Skipping {tbd_url}, similar to previous low data urls.")
                self.frontier.mark_url_complete(tbd_url, depth)
                self.frontier.add_low_data_url(tbd_url, 404)
                continue

            # check if url is similar to error urls
            if is_similar_url(tbd_url, error):
                self.logger.info(f"Skipping {tbd_url}, similar to previous error urls.")
                self.frontier.mark_url_complete(tbd_url, depth)
                self.frontier.add_error_url(tbd_url, 404)
                continue
                
            # check if meets common trap criteria
            is_trap, pattern = scraper.is_infinite_trap(tbd_url)
            if is_trap:
                self.logger.info(f"Skipping {tbd_url}, infinite trap detected {pattern}.")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            try:
                resp = download(tbd_url, self.config, self.logger)
            except Exception as e:
                self.logger.error(f"Error while downloading {tbd_url}: {str(e)}")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            # check if resp is a redirect or error
            if 300 <= resp.status <= 399:
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                redirect_url = resp.raw_response.headers.get('Location')
                if redirect_url is not None:
                    self.logger.info(f"Redirected {tbd_url} to {redirect_url}.")
                    self.frontier.add_url(redirect_url, depth)
                self.frontier.mark_url_complete(tbd_url, depth)
                continue
            elif resp.status != 200:
                self.frontier.add_error_url(tbd_url, resp.status)
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            # check if resp.raw_response is None
            if resp.raw_response is None:
                self.logger.info(f"Skipping {tbd_url}, empty raw_response.")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            # scrape the resp
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            try:
                scraped_urls, words, simhash = scraper.scraper(tbd_url, resp)
            except Exception as e:
                self.logger.error(f"Error while scraping {tbd_url}: {str(e)}")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue

            # check if there is very little content
            if words is not None and len(words) < self.min_words:
                self.logger.info(f"Skipping {tbd_url}, too few words.")
                url_hash = scraper.SimHash(words)
                self.frontier.add_low_data_url(tbd_url)
                self.frontier.mark_url_complete(tbd_url, depth)
                continue
            
            similar = False
            if simhash is not None:
                simhashes = self.frontier.get_simhashes()
                similar = self.is_similar(simhash, simhashes)
                self.frontier.add_simhash(simhash, tbd_url)

            if similar:
                self.logger.info(f"Skipping {tbd_url}, similar content.")
                self.frontier.mark_url_complete(tbd_url, depth)
                continue
            
            # add scraped words to frontier
            if words is not None:
                self.frontier.add_words(words, tbd_url)
            
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url, depth + 1)
            self.frontier.mark_url_complete(tbd_url, depth)
    
    def is_similar(self, new_simhash, simhashes):
        """
        Check if the new simhash is similar to any of the simhashes in the frontier.
        """
        if simhashes is None or new_simhash is None:
            return False

        for doc_id, old_hash in simhashes.items():
            if new_simhash.similarity(old_hash) > self.similarity_threshold:
                return True
        return False


def jaccard_similarity(url1, url2):
    """
    Calculate the Jaccard similarity of two URLs.
    Parameters:
        url1, url2 (str): URLs to compare.
    Returns:
        float: Jaccard similarity.
    """
    parsed1 = urlparse(url1)
    parsed2 = urlparse(url2)

    # If the domains/hosts are different, return 0
    if parsed1.netloc != parsed2.netloc:
        return 0

    set1 = set(parsed1.path.split('/')) | set((k, tuple(v)) for k, v in parse_qs(parsed1.query).items())
    set2 = set(parsed2.path.split('/')) | set((k, tuple(v)) for k, v in parse_qs(parsed2.query).items())

    intersection = set1 & set2
    union = set1 | set2

    return len(intersection) / len(union)



def is_similar_url(new_url, old_urls, threshold=0.95, similarity_count_threshold=5):
    """
    Check if a new URL is similar to any old URLs.
    
    Parameters:
        new_url (str): The new URL to check.
        old_urls (list or set): The old URLs to compare against.
        threshold (float): The Jaccard similarity threshold above which a URL is considered similar.
        similarity_count_threshold (int): The number of similar URLs above which a new URL is considered similar.

    Returns:
        bool: True if the new URL is similar to a significant number of old URLs, False otherwise.
    """
    similar_urls_count = 0
    for old_url in old_urls:
        if jaccard_similarity(new_url, old_url) >= threshold:
            similar_urls_count += 1
            if similar_urls_count >= similarity_count_threshold:
                return True

    return False



