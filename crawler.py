import logging
import re
from urllib.parse import urlparse, parse_qsl, urljoin

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.DynamicURLs = dict()
    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        soup = BeautifulSoup(url_data["content"], "lxml")
        identifiers = soup.find_all('a')
        for tag in identifiers:
            link = tag.get('href') # gets all the href links
            if type(link) != None:
                outputLinks.append(urljoin(url_data["url"], link)) # If the link is relative
            else: outputLinks.append(link) # If the link is absoulute
        return outputLinks


    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        # traps = set()
        # if url in traps:
        #     return False

        if parsed.scheme not in set(["http", "https"]):
            # traps.add(url)
            return False

        #Empty URLS
        if parsed is None or parsed == "":
            # traps.add(url)
            return False

        #Avoid calendars
        if "calendar" in parsed.path:
            # traps.add(url)
            return False

        #Avoid dynamic URLs
        static_link = url.split('?')[0]
        if static_link not in self.DynamicURLs:
            self.DynamicURLs[static_link] = 1
        else: self.DynamicURLs[static_link] += 1

        if self.DynamicURLs[static_link] > 700:
            return False


        #Long URLS
        if len(url.strip(".").strip("/")) > 300:
            # traps.add(url)
            return False

        # Continuously repeating subdirectories
        subdirSet = set(parsed.path.split("/"))
        subdirList = list(parsed.path.split("/"))
        while "" in subdirList:
            subdirList.remove("")
        while "" in subdirSet:
            subdirSet.remove("")
        if len(subdirSet) != len(subdirList):
            # traps.add(url)
            return False


        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False