import unittest
from collections import Counter
from unittest.mock import MagicMock, patch
from utils.config import Config
from utils import get_urlhash
from crawler.frontier import Frontier
from configparser import ConfigParser
from urllib.robotparser import RobotFileParser
from xml.etree import ElementTree as ET
from utils.response import Response
from pickle import dumps
from simhash import Simhash
from urllib.parse import urlparse
from queue import Queue
from time import time
import shelve
import os

class TestSimpleFrontier(unittest.TestCase):

    def setUp(self):
        config_file = "config.ini"
        cparser = ConfigParser()
        cparser.read(config_file)
        config = Config(cparser)
        config.seed_urls = ["https://www.stat.uci.edu/"]
        self.config = config

            # Patch download function and add cleanup to restore the original function
        patcher1 = patch("crawler.frontier.download", new=mocked_download)
        patcher1.start()
        self.addCleanup(patcher1.stop)

        # Patch download_robots_txt_parser_for_domain method and add cleanup to restore the original method
        patcher2 = patch("crawler.frontier.Frontier.download_robots_txt_parser_for_domain",
                        new=self.frontier_download_robots_txt_parser_for_domain)
        patcher2.start()
        self.addCleanup(patcher2.stop)

        self.frontier = Frontier(self.config, restart=True)
        words = Counter({'test': 1, 'word': 10, 'extra': 5})
        self.frontier.add_words(words, 'https://www.stat.uci.edu/')

        # with patch.object(Frontier, 'download_robots_txt_parser_for_domain',
        #                   side_effect=self.frontier_download_robots_txt_parser_for_domain):
        #     self.frontier = Frontier(self.config, restart=True)

    
    def frontier_download_robots_txt_parser_for_domain(self, domain):
        robots_txt_content = """User-agent: *
        Disallow: /wp-admin/
        Allow: /wp-admin/admin-ajax.php

        Sitemap: https://www.stat.uci.edu/wp-sitemap.xml"""

        mocked_parser = RobotFileParser()
        mocked_parser.parse(robots_txt_content.splitlines())
        return mocked_parser

    def test_add_words(self):
        initial_word_count = self.frontier.word_count.copy()
        words = Counter({'test': 3, 'word': 2})
        self.frontier.add_words(words, 'https://example.com')
        expected_word_count = initial_word_count + words
        self.assertEqual(self.frontier.word_count, expected_word_count)

    def test_mark_url_complete(self):
        test_url = "https://example.com"
        urlhash = get_urlhash(test_url)
        self.frontier.save[urlhash] = (test_url, False)
        self.frontier.save.sync()
        self.frontier.mark_url_complete(test_url)
        self.assertEqual(self.frontier.save[urlhash], (test_url, True))

    def test_is_similar(self):
        features1 = [('the', 1), ('cat', 1), ('ate', 1), ('food', 1)]
        features2 = [('the', 1), ('cat', 1), ('ate', 1), ('food', 1)]
        features3 = [('the', 1), ('armadillo', 1), ('rolled', 1), ('around', 1)]

        simhash1 = Simhash(features1)
        simhash2 = Simhash(features2)
        simhash3 = Simhash(features3)

        self.frontier.add_simhash('s1', simhash1)

        self.assertTrue(self.frontier.is_similar('s2', simhash2))
        self.assertFalse(self.frontier.is_similar('s3', simhash3))
    
    def test_add_url(self):
        new_url = "https://www.example.com/new_page"
        domain = urlparse(new_url).netloc
        with self.subTest("Test add new URL to frontier"):
            self.frontier.add_url(new_url)
            self.assertIn(new_url, (url for url, _ in self.frontier.save.values()))

        with self.subTest("Test add existing URL to frontier"):
            current_domain_queue_size = self.frontier.domains_to_scrape[domain].qsize()
            self.frontier.add_url(new_url)
            new_domain_queue_size = self.frontier.domains_to_scrape[domain].qsize()
            self.assertEqual(current_domain_queue_size, new_domain_queue_size)

    def test_get_tbd_url(self):
        test_url = "https://example.com/some_page"
        self.frontier.add_url(test_url)
        domain = urlparse(test_url).netloc

        with self.subTest("Test get next URL to be downloaded with delay"):
            stat_domain = 'www.stat.uci.edu'
            self.frontier.last_request_time[stat_domain] = time()
            url_to_be_downloaded = self.frontier.get_tbd_url()
            self.assertEqual(url_to_be_downloaded, test_url)

        with self.subTest("Test all expected URLs in the queue"):
            expected_urls = [
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
            # get all urls
            tbd_urls = []
            while url_to_be_downloaded is not None:
                url_to_be_downloaded = self.frontier.get_tbd_url()
                tbd_urls.append(url_to_be_downloaded)

            for expected_url in expected_urls:
                self.assertIn(expected_url, tbd_urls)

        with self.subTest("Test get next URL when the queue is empty"):
            self.frontier.domains_to_scrape[domain] = Queue()
            url_to_be_downloaded = self.frontier.get_tbd_url()
            self.assertIsNone(url_to_be_downloaded)
    
    def test_save_data(self):
        # Test data
        test_subdomains = {'www.stat.uci.edu': 11, 'www.example.com': 1}
        test_word_count = Counter({'test': 1, 'word': 10, 'extra': 5})
        test_max_words = 16



        self.frontier.subdomains = test_subdomains
        self.frontier.word_count = test_word_count
        self.frontier.max_words = test_max_words

        # Save the data
        self.frontier.save_data()

        # Load the saved data
        with shelve.open(self.config.save_file + '_data') as data_file:
            saved_subdomains = data_file['subdomains']
            saved_word_count = data_file['word_count']
            saved_max_words = data_file['max_words']

        # Remove the data file after loading
        os.remove(self.config.save_file + '_data.db')

        # Check if the saved data is equal to the test data
        self.assertEqual(saved_subdomains, test_subdomains)
        self.assertEqual(saved_word_count, test_word_count)
        self.assertEqual(saved_max_words, test_max_words)


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


# if __name__ == '__main__':
#     unittest.main()
