import unittest
from threading import Thread
from scraper import scraper, is_valid, count_words, extract_text_and_next_links
from utils.response import Response
from pickle import dumps
from nltk import download


class TestScraper(unittest.TestCase):
    def test_is_valid(self):
        self.assertTrue(is_valid("http://www.ics.uci.edu"))
        self.assertTrue(is_valid("http://www.cs.uci.edu"))
        self.assertTrue(is_valid("http://www.informatics.uci.edu"))
        self.assertTrue(is_valid("http://www.stat.uci.edu/?page_id=352#fragment"))
        self.assertTrue(is_valid("http://www.test.ics.uci.edu/ex.html#fake"))
        self.assertTrue(is_valid("http://www.anything.cs.uci.edu/"))
        self.assertTrue(is_valid("http://www.what.informatics.uci.edu"))
        self.assertTrue(is_valid("http://www.help.stat.uci.edu/"))
        self.assertFalse(is_valid("http://www.invalid_domain.com"))

    def test_count_words(self):
        text = "This is a sample text with some words in it."
        word_counter, simhash = count_words(text)
        self.assertEqual(word_counter["sample"], 1)
        self.assertEqual(word_counter["words"], 1)
        self.assertEqual(word_counter["text"], 1)
        self.assertIsNotNone(simhash)

    def test_extract_text_and_next_links(self):
        html = """
        <html>
            <head><title>Test Page</title></head>
            <body>
                <p>Some text here.</p>
                <a href="http://example.com/link1">Link 1</a>
                <a href="http://example.com/link2">Link 2</a>
            </body>
        </html>
        """
        url = "http://example.com"
        headers = {
            "Content-Type": "text/html",
            "Content-Length": "100"
        }
        mock_response = MockResponse(html, headers)
        pickled_html = dumps(mock_response)
        resp_dict = {
            "url": url,
            "status": 200,
            "response": pickled_html
        }
        response = Response(resp_dict)
        links, text = extract_text_and_next_links(url, response)
        self.assertIn("http://example.com/link1", links)
        self.assertIn("http://example.com/link2", links)
        self.assertIn("Some text here.", text)

    def test_scraper(self):
        html = """
        <html>
            <head><title>Test Page</title></head>
            <body>
                <p>Some text here.</p>
                <a href="http://informatics.uci.edu:650/link2"">Link 1</a>
                <a href="http://ics.uci.edu/link1?param=123">Link 2</a>
                <a href="http://stat.uci.edu/">Link 3</a>
                <a href="http://test.stat.uci.edu/api?test_param=44">Link 3</a>
                <a href="http://invalid.uci.edu/">Link 3</a>
                <a href="http://uci.edu/content">Link 3</a>

            </body>
        </html>
        """
        url = "http://uci.edu"
        headers = {
            "Content-Type": "text/html",
            "Content-Length": "100"
        }
        mock_response = MockResponse(html, headers)
        pickled_html = dumps(mock_response)
        resp_dict = {
            "url": url,
            "status": 200,
            "response": pickled_html
        }
        response = Response(resp_dict)
        links, words, simhash = scraper(url, response)
        self.assertIn("http://ics.uci.edu/link1?param=123", links)
        self.assertIn("http://informatics.uci.edu:650/link2", links)
        self.assertIn("http://stat.uci.edu/", links)
        # no invalid links
        self.assertNotIn("http://invalid.uci.edu/", links)
        self.assertNotIn("http://uci.edu/content", links)
        self.assertEqual(words["text"], 1)
        self.assertIsNotNone(simhash)


class MockResponse:
    def __init__(self, content, headers):
        self.content = content
        self.headers = headers

if __name__ == "__main__":
    download('stopwords')
    unittest.main()
