import logging
import re
from urllib.parse import urlparse, parse_qsl, urljoin, parse_qs
import tokenize
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
        self.queryStack = dict()
        self.MAXoutLinks = ('',0)

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

        """page = gettext(url_data)
        pagewords = page.split()
        matching_pgwords = [word for word in pagewords ]"""

        # keeps track of all the valid outlinks
        if len(outputLinks) > self.MAXoutLinks[1]:
            self.MAXoutLinks = (url_data["url"], len(outputLinks))
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        traps = set()

        # Trap counter, History-based detection
        if url in traps:
            return False

        if parsed.scheme not in set(["http", "https"]):
            traps.add(url)
            return False

        # Empty URLS
        if parsed is None or parsed == "":
            traps.add(url)
            return False

        # Avoid calendars
        if "calendar" in parsed.path:
            traps.add(url)
            return False

        # Avoid dynamic URLs
        static_link = url.split('?')[0]
        if static_link not in self.DynamicURLs:
            self.DynamicURLs[static_link] = 1
        else:
            self.DynamicURLs[static_link] += 1
        if self.DynamicURLs[static_link] > 700:
            traps.add(url)
            return False

        # Long URLS
        if len(url.strip(".").strip("/")) > 300:
            traps.add(url)
            return False

        # Continuously repeating subdirectories
        subdirSet = set(parsed.path.split("/"))
        subdirList = list(parsed.path.split("/"))
        while "" in subdirList:
            subdirList.remove("")
        while "" in subdirSet:
            subdirSet.remove("")
        if len(subdirSet) != len(subdirList):
            traps.add(url)
            return False

        # Anchor traps
        if "#" in url:
            traps.add(url)
            return False

        # Repeating query parameters
        queryParams = parse_qs(parsed.query)
        if len(queryParams.keys()) > 2:
            traps.add(url)
            traps.add(url)
            return False

        # Same root path/webpage, different content
        # root path = key, full path = value
        # if key is same and full path is diff, return false
        # else pop that root path stack implementation using a dict
        rootPathDict = {tuple(subdirList[:-1]): subdirList}
        # if next root path is same as last root path and full path is not = the the full path
        if tuple(subdirList[:-1]) in rootPathDict.keys() and subdirList != rootPathDict[tuple(subdirList[:-1])]:
            traps.add(url)
            return False
        else:
            rootPathDict.clear()

        #Same netloc, same query parameter, different query value
        if parsed.query != "" and len(list(queryParams.keys())):
            print(parsed)
            uniqueParam = list(queryParams.keys())[0]
            if len(self.queryStack) == 0:
                self.queryStack[uniqueParam] = queryParams[uniqueParam]
            if list(self.queryStack.keys())[0] == uniqueParam and queryParams.get(uniqueParam) != self.queryStack[uniqueParam]:
                traps.add(url)
                return False
            elif list(self.queryStack.keys())[0] != uniqueParam:
                self.queryStack.clear()

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

    # referenced from: https://matix.io/extract-text-from-webpage-using-beautifulsoup-and-python/
    def gettext(self,url_data):
        soup = BeautifulSoup(url_data["content"], "lxml")
        text = soup.find_all(text=True)

        out = ''
        black = ['[document]', 'noscript', 'header', 'html', 'meta', 'head', 'input', 'script', 'style', 'div', 'a',
                 'img']
        for t in text:
            if t.parent.name not in black and not re.match('<!-- .* -->', str(t.encode('utf-8'))):
                out += '{} '.format(t)
        return out
