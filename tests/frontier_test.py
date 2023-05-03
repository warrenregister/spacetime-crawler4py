import unittest
from utils import get_urlhash
from crawler.frontier import Frontier
from urllib.robotparser import RobotFileParser
from utils.config import Config
from configparser import ConfigParser
from collections import Counter
from threading import Thread
import time

from utils.response import Response
from unittest.mock import patch


def mock_download(url, config, logger=None):
    if url == "https://www.stat.uci.edu/robots.txt":
        content = b"User-agent: *\nDisallow: /wp-admin/\nSitemap: https://www.stat.uci.edu/wp-sitemap.xml"
        return Response({
            "url": url,
            "status": 200,
            "response": content
        })
    elif url == "https://www.stat.uci.edu/wp-sitemap.xml":
        return Response({
            "url": url,
            "content": b"""<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url>
                    <loc>https://www.stat.uci.edu/</loc>
                    <lastmod>2021-09-16T18:10:08+00:00</lastmod>
                </url>
                <url>
                    <loc>https://www.stat.uci.edu/somepage.html</loc>
                    <lastmod>2021-09-16T18:10:08+00:00</lastmod>
                </url>
            </urlset>""",
            "status": 200
        })
    else:
        return Response({
            "url": url,
            "content": b"",
            "status": 404
        })


# Do not run, it visits the cache and I cannot do that yet
class TestFrontier(unittest.TestCase):
    def setUp(self):
        cparser = ConfigParser()
        cparser.read("config.ini")
        self.config = Config(cparser)
        self.config.seed_urls = ["https://www.stat.uci.edu/"]
        self.config.cache_server = ("localhost", 12345)
        self.patcher = patch("crawler.frontier.download", side_effect=mock_download)
        self.mock_download = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_add_url(self):
        frontier = Frontier(self.config, restart=True)
        frontier.add_url("https://www.stat.uci.edu/somepage.html")
        self.assertTrue(frontier.domains["www.stat.uci.edu"].qsize() > 0)

    def test_get_tbd_url(self):
        frontier = Frontier(self.config, restart=True)
        url = frontier.get_tbd_url()
        self.assertIn(url, self.config.seed_urls)
        frontier.add_url("https://www.stat.uci.edu/somepage.html")
        url = frontier.get_tbd_url()
        self.assertEqual(url, "https://www.stat.uci.edu/somepage.html")

    def test_get_sitemap_urls_from_robots_txt(self):
        frontier = Frontier(self.config, restart=True)
        robots_txt_url = "https://www.stat.uci.edu/robots.txt"
        sitemap_urls = frontier.get_sitemap_urls_from_robots_txt(robots_txt_url)
        self.assertTrue(len(sitemap_urls) > 0)
        self.assertIn("https://www.stat.uci.edu/wp-sitemap.xml", sitemap_urls)

    def test_get_urls_from_sitemap(self):
        frontier = Frontier(self.config, restart=True)
        sitemap_url = "https://www.stat.uci.edu/wp-sitemap.xml"
        urls = frontier.get_urls_from_sitemap(sitemap_url)
        self.assertTrue(len(urls) > 0)
        self.assertIn("https://www.stat.uci.edu/", urls)

    def test_process_stat_uci_edu_robots_txt(self):
        frontier = Frontier(self.config, restart=True)
        robots_txt_url = "https://www.stat.uci.edu/robots.txt"
        parser = frontier.get_robots_txt_parser(robots_txt_url)

        # Test disallowed URLs
        self.assertFalse(parser.can_fetch("*", "https://www.stat.uci.edu/wp-admin/"))

        # Test allowed URLs
        self.assertTrue(parser.can_fetch("*", "https://www.stat.uci.edu/somepage.html"))

    def download_robots_txt_parser_for_domain(self):
        domain = "www.stat.uci.edu"
        frontier = Frontier(self.config, restart=True)
        parser = frontier.download_robots_txt_parser_for_domain(domain)
        self.assertIsInstance(parser, RobotFileParser)

    def test_add_words(self):
        frontier = Frontier(self.config, restart=True)
        words = Counter({"statistics": 2, "data": 1})
        frontier.add_words(words)
        self.assertEqual(frontier.word_count, words)

    def test_mark_url_complete(self):
        frontier = Frontier(self.config, restart=True)
        url = "https://www.stat.uci.edu/somepage.html"
        frontier.add_url(url)
        urlhash = get_urlhash(url)
        frontier.mark_url_complete(url)
        self.assertTrue(frontier.save[urlhash][1])
    
    def test_thread_safety(self):
        frontier = Frontier(self.config, restart=True)

        # Define worker function that adds URLs to frontier
        def worker():
            for i in range(100):
                url = f"https://www.stat.uci.edu/page{i}.html"
                frontier.add_url(url)

        # Create multiple worker threads
        threads = []
        for i in range(self.config.threads_count):
            t = Thread(target=worker)
            threads.append(t)

        # Start worker threads
        for t in threads:
            t.start()

        # Wait for worker threads to finish
        for t in threads:
            t.join()

        # Assert that all URLs have been added to the frontier
        self.assertEqual(frontier.word_count["stat"], 100)
        self.assertEqual(frontier.word_count["uci"], 100)

        # Assert that save file has been updated with all URLs
        with frontier.lock:
            self.assertEqual(len(frontier.save), 100 * self.config.threads_count)

        # Define worker function that marks URLs as completed
        def worker2():
            for i in range(100):
                url = f"https://www.stat.uci.edu/page{i}.html"
                frontier.mark_url_complete(url)

        # Create multiple worker threads
        threads = []
        for i in range(self.config.threads_count):
            t = Thread(target=worker2)
            threads.append(t)

        # Start worker threads
        for t in threads:
            t.start()

        # Wait for worker threads to finish
        for t in threads:
            t.join()

        # Assert that all URLs have been marked as completed
        with frontier.lock:
            for urlhash, (url, completed) in frontier.save.items():
                self.assertTrue(completed)

        # Define worker function that retrieves URLs from the frontier
        def worker3():
            for i in range(100):
                url = frontier.get_tbd_url()
                time.sleep(0.01)

        # Create multiple worker threads
        threads = []
        for i in range(self.config.threads_count):
            t = Thread(target=worker3)
            threads.append(t)

        # Start worker threads
        for t in threads:
            t.start()

        # Wait for worker threads to finish
        for t in threads:
            t.join()

        # Assert that all URLs have been retrieved from the frontier
        with frontier.lock:
            self.assertEqual(len(frontier.save), 0)

if __name__ == "__main__":
    unittest.main()
