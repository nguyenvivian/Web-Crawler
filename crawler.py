# Benjamin Huynh 18686357
# Sergei Danielian 40124849
# Vivian Nguyen 84955920
import logging
import re
from urllib.parse import urlparse, parse_qsl, urljoin, parse_qs
import tokenize
from bs4 import BeautifulSoup
import requests

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
        self.subdomain = dict()
        self.wordsDict = dict()
        self.MAXoutLinks = ('', 0)
        self.downloadedURLs = []
        self.identifiedTraps = []
        # self.subdomainList = []
        self.traps = set()
        self.prevURLFlag = False
        self.subdomainCount = 0
        self.prevURL = ""
        self.prevPath = ""
        self.traps = set()
        self.longestPageLink = ""
        self.longestPageCount = 0
        self.stopWords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most',"mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves','out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves']

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            self.downloadedURLs.append(url)
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched,
                        len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        # analytics 1 - subdomain count ## do we need to print all the different subdomains to a list? or just the count??
        file = open("subdomainList.txt", "w+")
        file.write("Subdomains visited: \n")
        for k,v in self.subdomain.items():
            output = str(k) + ": " + str(v) + "\n"
            file.write(output)  # write the dictionary to a file
        file.close()

        # analytics 2 - valid outlinks
        outlinksFile = open("validOutlinks.txt", "w+")
        outLinkOutput = "The link with the most valid out links is " + str(self.MAXoutLinks[0]) + " with " + str(self.MAXoutLinks[1]) + " out links"
        outlinksFile.write(outLinkOutput)
        outlinksFile.close()

        # analytics 3 - downloads
        with open('downloadedURLs.txt', 'w+') as file:
            for item in self.downloadedURLs:
                file.write("%s\n" % item)
        file.close()

        # analytics 3 - traps
        with open('traps.txt', 'w+') as file:
            for item in self.traps:
                file.write("%s\n" % item)
        file.close()

        # analytics 4 -
        #longestPageList = list(sorted(self.wordsDict.items(), key=lambda k: (k[1]))) # this assignment and gettext is called at the end of isvalid function.

        with open('longestPage.txt', 'w+') as file:
            file.write("The longest page is ")
            file.write(longestPageLink)
            file.write("with ")
            file.write(str(longestPageCount))
            file.write(" amount of words.")

        # analytics 5 -
        # loop through wordsDict and count the number of words
        sortedWordSet = sorted(self.wordsDict.items(), key = lambda kv: (kv[1]), reverse= True)


        with open('MostCommonWords.txt', 'w') as file:
            for x in range(50):
                file.write(sortedWordSet[x][0])
                file.write("\n")
        file.close()

        # print(r, '\t', wdict[r]) # prints the dictionary with a tab space between it.

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
            link = tag.get('href')  # gets all the href links
            if type(link) != None:
                outputLinks.append(urljoin(url_data["url"], link))  # If the link is relative
            else:
                outputLinks.append(link)  # If the link is absoulute

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

        if parsed.hostname is not None and ".ics.uci.edu" in parsed.hostname: # ensure that we are only gathering subdomains from .ics.uci.edu
            # analytics 1
            if parsed.netloc not in self.subdomain:
                self.subdomain[parsed.netloc] = 1
            else:
                self.subdomain[parsed.netloc] += 1

        # Trap counter, History-based detection
        if url in self.traps:
            return False

        if parsed.scheme not in set(["http", "https"]):
            self.traps.add(url)
            return False

        # Empty URLS
        if parsed is None or parsed == "":
            self.traps.add(url)
            return False

        # Avoid calendars
        if "calendar" in parsed.path:
            self.traps.add(url)
            return False

        # Avoid dynamic URLs
        static_link = url.split('?')[0]
        if static_link not in self.DynamicURLs:
            self.DynamicURLs[static_link] = 1
        else:
            self.DynamicURLs[static_link] += 1
        if self.DynamicURLs[static_link] > 700:
            self.traps.add(url)
            return False

        # Long URLS
        if len(url.strip(".").strip("/")) > 300:
            self.traps.add(url)
            return False

        # Continuously repeating subdirectories
        subdirSet = set(parsed.path.split("/"))
        subdirList = list(parsed.path.split("/"))
        while "" in subdirList:
            subdirList.remove("")
        while "" in subdirSet:
            subdirSet.remove("")
        if len(subdirSet) != len(subdirList):
            self.traps.add(url)
            return False

        # Anchor traps
        if "#" in url:
            self.traps.add(url)
            return False

        # Repeating query parameters
        queryParams = parse_qs(parsed.query)
        if len(queryParams.keys()) > 2:
            self.traps.add(url)
            self.traps.add(url)
            return False

        # Same root path/webpage, different content
        # root path = key, full path = value
        # if key is same and full path is diff, return false
        # else pop that root path stack implementation using a dict
        rootPathDict = {tuple(subdirList[:-1]): subdirList}
        # if next root path is same as last root path and full path is not = the the full path
        if tuple(subdirList[:-1]) in rootPathDict.keys() and subdirList != rootPathDict[tuple(subdirList[:-1])]:
            self.traps.add(url)
            return False
        else:
            rootPathDict.clear()

        # Same netloc, same query parameter, different query value
        if parsed.query != "" and len(list(queryParams.keys())):
            uniqueParam = list(queryParams.keys())[0]
            if len(self.queryStack) == 0:
                self.queryStack[uniqueParam] = queryParams[uniqueParam]
            if list(self.queryStack.keys())[0] == uniqueParam and queryParams.get(uniqueParam) != self.queryStack[
                uniqueParam]:
                self.traps.add(url)
                return False
            elif list(self.queryStack.keys())[0] != uniqueParam:
                self.queryStack.clear()

        # analytics 4
        wordset = self.gettext(url)  # or should it be self.wordsDict[parsed.path] = gettext(url) ???????
        wordSum = sum(wordset.values())
        if (wordSum > self.longestPageCount):
            self.longestPageCount = wordSum
            self.longestPageLink = url

        # analytics 5
        sortedWordSet = sorted(wordset.items(), key = lambda kv: (kv[1]), reverse= True)
        count = 0
        keyCount = 0

        for x in sortedWordSet:
            if x[0] not in self.stopWords:
                if x[0] not in self.wordsDict:
                    self.wordsDict[x[0]] = x[1]
                else:
                    self.wordsDict[x[0]] += x[1]

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
    def gettext(self, url):
        wordCounter = dict()
        res = requests.get(url)
        html_page = res.content
        soup = BeautifulSoup(html_page, 'html.parser')
        text = soup.find_all(text=True)

        out = ''
        black = ['[document]', 'noscript', 'header', 'html', 'meta', 'head', 'input', 'script', 'style', 'div', 'a',
                 'img']
        for t in text:
            if t.parent.name not in black and not re.match('<!-- .* -->', str(t.encode('utf-8'))):
                out += '{} '.format(t)

        out = out.replace(" - ", ' ')  # replaces punctuation # might need to do
        out = out.replace(" . ", ' ')  # replaces punctuation
        out = out.replace(" ! ", ' ')  # replaces punctuation
        out = out.replace(" ; ", ' ')  # replaces punctuation
        out = out.replace(" : ", ' ')  # replaces punctuation
        out = out.replace('\n', ' ')   # replaces new line
        # replace any other puntuation thats not a-z 0-9 and is surrounded by white spaces. Ex: "hello - there " - > "hello  there"

        wordlist = out.split()  # split by whitespace
        for word in wordlist:
            if word not in wordCounter:
                wordCounter[word] = 1
            else:
                wordCounter[word] += 1

        # assign to dictionary with the key URL and the value as the wordCounter

        return wordCounter  # returns the numbe of words on this url to a dictionary, then use max() on that dict