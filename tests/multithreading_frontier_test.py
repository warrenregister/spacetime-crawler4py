# import threading
# import unittest
# import responses


# class TestMultithreading(unittest.TestCase):
#     @responses.activate
#     def setUp(self):
#         # Mock the sitemap and robots.txt files for domain1.com
#         responses.add(responses.GET, 'https://domain1.com/robots.txt',
#                     body="User-agent: *\nDisallow: /private\nSitemap: https://domain1.com/sitemap.xml")
#         responses.add(responses.GET, 'https://domain1.com/sitemap.xml',
#                     body="<urlset><url><loc>https://domain1.com/page1</loc></url></urlset>")

#         # Mock the sitemap and robots.txt files for domain2.com
#         responses.add(responses.GET, 'https://domain2.com/robots.txt',
#                     body="User-agent: *\nDisallow: /secret\nSitemap: https://domain2.com/sitemap.xml")
#         responses.add(responses.GET, 'https://domain2.com/sitemap.xml',
#                     body="<urlset><url><loc>https://domain2.com/page1</loc></url></urlset>")

#     def test_add_url_multithreading(self):
#         crawler = MyCrawler()

#         def worker():
#             for i in range(100):
#                 crawler.add_url("https://domain1.com/page{}".format(i))

#         threads = []
#         for _ in range(10):
#             thread = threading.Thread(target=worker)
#             threads.append(thread)
#             thread.start()

#         for thread in threads:
#             thread.join()

#         self.assertEqual(crawler.queue.qsize(), 1000)
