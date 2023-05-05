import unittest
from unittest.mock import patch
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from crawler.worker import Worker
from crawler.frontier import Frontier
from configparser import ConfigParser
from utils.config import Config
from utils.response import Response
import random
import string
from collections import Counter
from nltk.corpus import stopwords
from pickle import dumps
from utils import get_urlhash
import os
import pytest
import shelve
import time

request_timestamps = {}


class TestMultithreadingWorker(unittest.TestCase):

    def setUp(self, restart=True):
        random.seed(10)
        config_file = "config.ini"
        cparser = ConfigParser()
        cparser.read(config_file)
        config = Config(cparser)
        self.config = config
        self.config.threads_count = 3  # Adjust the number of worker threads to test
        config.seed_urls = ["https://www.stat.uci.edu/"]
        self.config = config

        self.expected_urls = [
                'https://www.stat.uci.edu/events/test-event-1',
                'https://www.stat.uci.edu/events/this',
                'https://www.stat.uci.edu/events/future-event',
                'https://www.stat.uci.edu/category/uncategorized',
                'https://www.stat.uci.edu/category/noteworthy-achievement',
                'https://www.stat.uci.edu/category/slider',
                'https://www.stat.uci.edu/category/article',
                'https://www.stat.uci.edu/author/coby',
                'https://www.stat.uci.edu/author/fontejon',
                'https://www.stat.uci.edu/author/matt',
                'https://www.stat.uci.edu'
            ]
        
        self.valid_domains = ['https://www.stat.uci.edu', 'https://www.ics.uci.edu', 'https://www.informatics.uci.edu', 'https://cs.uci.edu']
        self.valid_domains = [urlparse(url).netloc for url in self.valid_domains]

        if restart is True:
            self.link_windows, self.url_text_data = self._generate_random_parts()
            self.unique_links_in_windows = set(link for window in self.link_windows for link in window)

        # Patch download function for worker and frontier
        patcher1 = patch("crawler.worker.download", new=lambda url,
                         config, logger: worker_mocked_download(url, config,
                                                        self.link_windows,
                                                        self.url_text_data,
                                                        self.expected_urls))
        patcher1.start()
        self.addCleanup(patcher1.stop)


        patcher2 = patch("crawler.frontier.download", new=mocked_download)
        patcher2.start()
        self.addCleanup(patcher2.stop)

       # Patch download_robots_txt_parser_for_domain method and add cleanup to restore the original method
        patcher3 = patch("crawler.frontier.Frontier.download_robots_txt_parser_for_domain",
                        new=self.frontier_download_robots_txt_parser_for_domain)
        patcher3.start()
        self.addCleanup(patcher3.stop)


        self.frontier = Frontier(self.config, restart)

    def tearDown(self):
        if os.path.exists(self.config.save_file + '.db'):
            os.remove(self.config.save_file + '.db')

        if os.path.exists(self.config.save_file + '_data' + '.db'):
            os.remove(self.config.save_file + '_data' + '.db')
    
    def frontier_download_robots_txt_parser_for_domain(self, domain):
        robots_txt_content = """User-agent: *
        Disallow: /wp-admin/
        Allow: /wp-admin/admin-ajax.php

        Sitemap: https://www.stat.uci.edu/wp-sitemap.xml"""

        mocked_parser = RobotFileParser()
        mocked_parser.parse(robots_txt_content.splitlines())
        return mocked_parser


    def _generate_random_parts(self):

        # Generate random approved links
        def generate_links(n, domains):
            return [f"https://{random.choice(domains)}/{random.randint(1, 10)}#frag{random.randint(1, 1000)}" for _ in range(n)]

        # Generate random text for each expected URL
        def generate_text():
            words = ["".join(random.choices(string.ascii_letters, k=random.randint(1, 26))) for _ in range(random.randint(20, 100))]
            return " ".join(words)

        # Generate random approved links
        approved_links = generate_links(60, ["www.stat.uci.edu", "www.ics.uci.edu", "www.informatics.uci.edu"])

        # Generate random not approved links
        not_approved_links = generate_links(20, ["www.math.uci.edu", "www.eee.uci.edu", "www.eng.uci.edu"])
        # Combine approved and not approved links
        combined_links = approved_links + not_approved_links

        # Shuffle the combined links
        random.shuffle(combined_links)

        # Split the shuffled links into windows
        window_size = 10
        step_size = 3
        link_windows = [combined_links[i:i + window_size] for i in range(0, len(self.expected_urls * step_size), step_size)]

        # Generate random text for each expected URL
        url_text_data = {url: generate_text() for url in self.expected_urls}
        stop_words = set(stopwords.words('english'))

        # Count non-stop words for each URL
        non_stop_words_count = {url: Counter([word for word in text.split() if word.lower() not in stop_words]) for url, text in url_text_data.items()}

        # Save the generated data
        url_text_data = {
            url: {
                "text": text,
                "counter": non_stop_words_count[url],
                "total_words": sum(non_stop_words_count[url].values())

            } for url, text in url_text_data.items()
        }

        return link_windows, url_text_data

    @pytest.mark.run(order=1)
    def test_worker_run(self):
        workers = [Worker(worker_id=i, config=self.config, frontier=self.frontier)
                   for i in range(self.config.threads_count)]  # Spawn multiple workers
        for worker in workers:
            worker.start()

        for worker in workers:
            worker.join()

        # Check if each link in unique_links_in_windows is in the frontier if it is valid
        for link in self.unique_links_in_windows:
            parsed_url = urlparse(link)
            domain = parsed_url.netloc
            if domain in self.valid_domains:
                self.assertIn(get_urlhash(link), self.frontier.save)

        # Check if the max words URL is the one with the highest total words in url_text_data
        max_words_url = max(self.url_text_data, key=lambda x: self.url_text_data[x]["total_words"])
        self.assertEqual(self.frontier.max_words[0], max_words_url)

        # Check if every approved subdomain in unique_links_in_windows is in the subdomain dict with a set of associated URLs
        for link in self.unique_links_in_windows:
            parsed_url = urlparse(link)
            # remove fragment from link
            link = parsed_url._replace(fragment="").geturl()
            domain = parsed_url.netloc
            if domain in self.valid_domains:
                subdomain = parsed_url.hostname
                self.assertIn(subdomain, self.frontier.subdomains)
                self.assertIn(link, self.frontier.subdomains[subdomain])
        
        # Check if politeness delay is respected for each domain
        politeness_delay = self.config.time_delay
        for domain, timestamps in request_timestamps.items():
            for i in range(1, len(timestamps)):
                time_difference = timestamps[i] - timestamps[i-1]
                self.assertGreaterEqual(time_difference, politeness_delay)
    
    @classmethod
    def run_ordered_tests(cls):
        suite = unittest.TestSuite()
        suite.addTest(cls('test_worker_run'))
        suite.addTest(cls('test_load_saved_data'))
        unittest.TextTestRunner().run(suite)

    @pytest.mark.run(order=2)
    def test_load_saved_data(self, frontier=None):

        self.test_worker_run()        
        # Reinitialize Frontier and Worker instances without restarting (loading the saved data)
        self.setUp(restart=False)

        # Perform the checks against the loaded data structures
        # Replace `self.frontier.subdomains`, `self.frontier.word_count`, and `self.frontier.max_words`
        # with the loaded data from the shelves.
        with shelve.open(self.config.save_file + '_data') as data_file:
            loaded_subdomains = data_file['subdomains']
            loaded_word_count = data_file['word_count']
            loaded_max_words = data_file['max_words']

        self.assertEqual(loaded_subdomains, self.frontier.subdomains)
        self.assertEqual(loaded_word_count, self.frontier.word_count)
        self.assertEqual(loaded_max_words, self.frontier.max_words)
        



def read_file_content_as_bytes(file_path):
    with open(file_path, "rb") as file:
        content = file.read()
    return content


def mocked_download(url, config):
    resp_dict = {'url': url, 'status': 200}

    # Update these paths to the actual paths of your saved files
    sitemap_index_path = "./template_files/www.stat.uci.eduwp-sitemap.txt"
    sitemap_1_path = "./template_files/www.stat.uci.eduwp-sitemap-users-1.txt"
    sitemap_2_path = "./template_files/www.stat.uci.eduwp-sitemap-taxonomies-category-1.txt"
    sitemap_3_path = "./template_files/www.stat.uci.eduwp-sitemap-posts-events-1.txt"

    xml_mapping = {
        "https://www.stat.uci.edu/wp-sitemap.xml": read_file_content_as_bytes(sitemap_index_path),
        "https://www.stat.uci.edu/wp-sitemap-users-1.xml": read_file_content_as_bytes(sitemap_1_path),
        "https://www.stat.uci.edu/wp-sitemap-taxonomies-category-1.xml": read_file_content_as_bytes(sitemap_2_path),
        "https://www.stat.uci.edu/wp-sitemap-posts-events-1.xml": read_file_content_as_bytes(sitemap_3_path),
    }

    # catch missing keys
    try:
        resp_dict['response'] = dumps(xml_mapping[url])
    except KeyError:
        resp_dict['status'] = 404
        resp_dict['response'] = dumps("Not Found")

    return Response(resp_dict)


def worker_mocked_download(url, config, link_windows, url_text_data, expected_urls):
    resp_dict = {'url': url, 'status': 200}
    # Record request timestamps for each domain
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    current_time = time.time()
    if domain not in request_timestamps:
        request_timestamps[domain] = []
    request_timestamps[domain].append(current_time)
    # Prepare template HTML content for each expected URL
    template_html = '''
    <html>
    <head><title>Example Page</title></head>
    <body>
        <p>{text}</p>
        {links}
    </body>
    </html>
    '''
    headers = {
        "Content-Type": "text/html; charset=UTF-8",
        "Server": "Example-Server"
    }

    # Map expected URLs to their template HTML content and headers
    html_mapping = {
        expected_url: {
            "content": template_html,
            "headers": headers
        } for expected_url in expected_urls
    }

    # Catch missing keys
    try:
        i = expected_urls.index(url)
        window = link_windows[i]
        links = "\n".join(f'<a href="{link}">{link}</a>' for link in window)
        content = html_mapping[url]["content"].format(text=url_text_data[url]["text"], links=links)

        # determine content length
        mock_response = MockResponse(content, headers)
        pickled_html = dumps(mock_response)
        headers["Content-Length"] = len(content)
        resp_dict['response'] = pickled_html
        resp_dict['headers'] = headers
    except KeyError and ValueError:
        resp_dict['status'] = 404
        resp_dict['response'] = dumps("Not Found")

    response = Response(resp_dict)
    return response


class MockResponse:
    def __init__(self, content, headers):
        self.content = content
        self.headers = headers



if __name__ == '__main__':
    TestMultithreadingWorker.run_ordered_tests()
