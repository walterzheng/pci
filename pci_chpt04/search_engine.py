# -*- coding: utf-8 -*-
"""
Created on Wed Apr 06 22:51:56 2016

@author: walterzheng
"""

import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import sqlite3 as sqlite
import socket
import re
import os

splitter = re.compile('\\W*')
ignorewords = ("a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the")


class Crawler:
    def __init__(self, dbname, rebuild_db=False):
        self.dbname = dbname
        self.con = sqlite.connect(dbname)
        if rebuild_db:
            self.create_index_table()
    
    def __del__(self):
        self.commit()
        self.con.close()
    
    def dbcommit(self):
        self.con.commit()
    
    def get_entry_id(self, table, field, value, create_new=True):
        cur = self.con.execute("select * from %s where %s='%s'"%(table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute("insert into %s (%s) values('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]
        
    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return
        print "Indexing %s" %url
        txt = self.get_text(soup)
        words = self.separate_text(txt)
        
        urlid = self.get_entry_id("urllist", "url", url)
        
        for i, word in enumerate(words):
            if word in ignorewords:
                continue
            wordid = self.get_entry_id("wordlist", "word", word)
            print urlid
            print wordid
            print i
            self.con.execute("insert into wordlocation(urlid, wordid, location) \
                                values (%d,%d,%d)" % (urlid, wordid, i))
        
    def get_text(self, soup):
        rtn_str = soup.string
        if rtn_str:
            #print "get_text1:%s" %rtn_str.strip()
            return rtn_str.strip()
        else:
            rtn_str = ''
            for child in soup.children:
                subtxt = self.get_text(child)
                rtn_str += subtxt + '\n'
            #print "get_text2:%s" %rtn_str
            return rtn_str
        
    def separate_text(self, text):
        return [s.lower() for s in splitter.split(text) if s != '']
        
    def is_indexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'"%(url)).fetchone()
        if u != None:
            v = self.con.execute("select wordid from wordlocation where urlid=%d"%u[0]).fetchone()
            if v != None:
                return True
        return False
        
    def add_link_ref(self, url_from, url_to, link_text):
        fromid = self.get_entry_id("urllist", "url", url_from)
        toid = self.get_entry_id("urllist", "url", url_to)
        
        linkid = self.con.execute("insert into link (fromid, toid) \
                                values (%d,%d)" % (fromid, toid)).lastrowid
                                
        words = self.separate_text(link_text)
        for i, word in enumerate(words):
            if word in ignorewords:
                continue
            wordid = self.get_entry_id("wordlist", "word", word)
            self.con.execute("insert into linkwords(wordid, linkid) \
                                values (%d,%d)" % (wordid, linkid))
    
    def crawler(self, pages, depth=2, timeout=10):
        search_set = set(pages)
        socket.setdefaulttimeout(timeout)
        for round in xrange(depth):
            new_search_set = set()
            for page in search_set:
                try:
                    f = urllib2.urlopen(page)
                except:
                    print "can't open %s"%page
                    continue
                soup = BeautifulSoup(f)
                
                self.add_to_index(page, soup)
                links = soup.find_all("a")
                for link in links:
                    if "href" in link.attrs:
                        url = urljoin(page, link["href"])
                        if url.startswith('http') and not self.is_indexed(url):
                            new_search_set.add(url)
                        #print url
                        link_txt = self.get_text(link)
                        self.add_link_ref(page, url, link_txt)
                self.dbcommit()
                            
            search_set = new_search_set
                            
    
    def create_index_table(self):
        self.dbcommit()
        self.con.close()
        if os.path.exists(self.dbname):
            os.remove(self.dbname)
        self.con = sqlite.connect(self.dbname)
        
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index wordlocidx on wordlocation(wordid)')
        self.con.execute('create index fromidx on link(fromid)')
        self.con.execute('create index toidx on link(toid)')
        
        self.dbcommit()
    
if __name__ == '__main__':
    crawler = None
    try:
        pages = ['http://en.wikipedia.org/wiki/Vratitsa']
        crawler = Crawler("haha.db", rebuild_db=True)
        crawler.crawler(pages, depth=1)
    finally:
        del crawler