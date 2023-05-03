from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time


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
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    def run(self):
        """
        Run the worker loop until there are no more URLs to be downloaded.
        """
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if tbd_url is None:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            resp = download(tbd_url, self.config, self.logger)

            # check if resp is a redirect or error
            if 300 <= resp.status <= 399:
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                redirect_url = resp.raw_response.headers.get('Location')
                if redirect_url:
                    self.logger.info(f"Redirected {tbd_url} to {redirect_url}.")
                    self.frontier.add_url(redirect_url)
                self.frontier.mark_url_complete(tbd_url)
                continue
            elif resp.status != 200:
                self.logger.info(f"Skipping {tbd_url}, status <{resp.status}>.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            # scrape the resp
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            try:
                scraped_urls, words, minhash = scraper.scraper(tbd_url, resp)
            except Exception as e:
                self.logger.error(f"Error while scraping {tbd_url}: {str(e)}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            similar = False
            if minhash is not None:
                similar = self.frontier.is_similar(minhash)
            
            # add scraped words to frontier
            if words is not None and not similar:
                self.frontier.add_words(words)
            
            if not similar:
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url, minhash)
            self.frontier.mark_url_complete(tbd_url)
