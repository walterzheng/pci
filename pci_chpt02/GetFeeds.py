# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 15:38:53 2016

@author: walterzheng
"""

import urllib
import feedparser
import re
import pickle 
import socket

url_ro = re.compile(r"""(?<=href)\s*?=\s*?(?P<quote>['"])(https?:[/\.\w\d]*)(?P=quote)""")

def GetFeedsFromURL(url, timeout = 300):
    socket.setdefaulttimeout(timeout)    
    
    content = urllib.urlopen(url).read()
    ref_list = url_ro.findall(content)
    
    feeds_list = []
    ref_set = set()
    for var, ref in ref_list:
        ref_set.add(ref)
        
    for ref in ref_set:
        print "\t%s"%ref
        try:
            rtn = feedparser.parse(ref)
        except:
            continue
        if rtn.bozo == 0:
            feeds_list.append((ref, rtn))
            
    return feeds_list
    
if __name__ == "__main__":
#    rtn = GetFeedsFromURL("http://www.uen.org/feeds/lists.shtml")
#    for line in rtn:
#        print line.feed.title
#    with open("feeds_dump", "w") as f:
#        pickle.dump(rtn, f)
    
    categories = [("Arts", 287), ("Business", 167), ("Computers", 218), ("Education", 5), \
        ("Entertainment", 10), ("Games", 24), ("Health", 72), ("Home", 51), ("Kids_and_Teens", 7), \
        ("Lifestyle", 5), ("News", 118), ("Recreation", 125), ("Reference", 47), \
        ("Regional", 1284), ("Science", 131), ("Shopping", 6), ("Society", 200), \
        ("Sports", 214), ("World", 1005)]
        
    for cate, num in categories:
        for index in xrange(1, num/20 + 2):
            url = "http://www.rssfeeds.org/rss.go/rss-feeds/%s.html?page=%d"%(cate, index)
            print url
            rtn = GetFeedsFromURL(url)
#            for data in rtn:
#                print data[0]
            with open("feeds_dump/%s"%cate, "w") as f:
                pickle.dump(rtn, f)