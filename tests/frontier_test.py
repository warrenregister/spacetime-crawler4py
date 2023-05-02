import unittest
from utils import get_urlhash
from crawler.multi_thread_frontier import (Frontier,
                                           get_urls_from_sitemap,
                                           get_robots_txt_parser_for_domain)
from urllib.robotparser import RobotFileParser
from utils.config import Config
from configparser import ConfigParser
from collections import Counter


class TestFrontier(unittest.TestCase):
    def setUp(self):
        cparser = ConfigParser()
        cparser.read("config.ini")
        self.config = Config(cparser)
        self.config.seed_urls = ["https://www.stat.uci.edu/"]

    def test_add_url(self):
        frontier = Frontier(self.config, restart=True)
        frontier.add_url("https://www.stat.uci.edu/somepage.html")
        self.assertTrue(frontier.domains["www.stat.uci.edu"].qsize() > 0)

    def test_get_tbd_url(self):
        frontier = Frontier(self.config, restart=True)
        frontier.add_url("https://www.stat.uci.edu/somepage.html")
        url = frontier.get_tbd_url()
        url = frontier.get_tbd_url()
        self.assertEqual(url, "https://www.stat.uci.edu/somepage.html")

    def test_process_stat_uci_edu_robots_txt(self):
        frontier = Frontier(self.config, restart=True)
        robots_txt_url = "https://www.stat.uci.edu/robots.txt"
        parser = frontier.get_robots_txt_parser(robots_txt_url)

        # Test disallowed URLs
        self.assertFalse(parser.can_fetch("*", "https://www.stat.uci.edu/wp-admin/"))

        # Test allowed URLs
        self.assertTrue(parser.can_fetch("*", "https://www.stat.uci.edu/somepage.html"))

    def test_get_sitemap_urls_from_robots_txt(self):
        frontier = Frontier(self.config, restart=True)
        robots_txt_url = "https://www.stat.uci.edu/robots.txt"
        sitemap_urls = frontier.get_sitemap_urls_from_robots_txt(robots_txt_url)
        self.assertTrue(len(sitemap_urls) > 0)
        self.assertIn("https://www.stat.uci.edu/wp-sitemap.xml", sitemap_urls)

    def test_get_urls_from_sitemap(self):
        sitemap_url = "https://www.stat.uci.edu/wp-sitemap.xml"
        urls = get_urls_from_sitemap(sitemap_url)
        self.assertTrue(len(urls) > 0)
        self.assertIn("https://www.stat.uci.edu/", urls)

    def test_get_robots_txt_parser_for_domain(self):
        domain = "www.stat.uci.edu"
        parser = get_robots_txt_parser_for_domain(domain)
        self.assertIsInstance(parser, RobotFileParser)

    def test_add_words(self):
        frontier = Frontier(self.config, restart=True)
        words = Counter({"statistics": 2, "data": 1})
        frontier.add_words(words)
        self.assertEqual(frontier.word_count, words)

    def test_add_subdomain(self):
        frontier = Frontier(self.config, restart=True)
        subdomain = "subdomain.example.com"
        frontier.add_subdomain(subdomain)
        self.assertIn(subdomain, frontier.subdomains)

    def test_mark_url_complete(self):
        frontier = Frontier(self.config, restart=True)
        url = "https://www.stat.uci.edu/somepage.html"
        frontier.add_url(url)
        urlhash = get_urlhash(url)
        frontier.mark_url_complete(url)
        self.assertTrue(frontier.save[urlhash][1])
