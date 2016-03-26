# -*- coding: utf-8 -*-
"""
Created on Sat Mar 19 19:11:37 2016

@author: walterzheng
"""

import re
import collections

html_token = re.compile(r'<[^>]+>')
not_word = re.compile(r'[^A-Z^a-z]+')

def GetWords(feed_desc):
    txt = html_token.sub(' ', feed_desc)
    words = not_word.split(txt)
    
    return [word.lower() for word in words if word]
    
def GetWordCount(feed):
    wc = collections.defaultdict(int)
    
    for e in feed.entries:
        if 'summary' in e:
            summary = e.summary
        elif 'description' in e:
            summary = e.description
        else:
            continue
            
        words = GetWords(e.title + ' ' + summary)
        
        for word in words:
            wc[word] += 1
    
    if 'title' in feed.feed:
        return feed.feed.title, wc
    else:
        return None, {}
    
if __name__ == '__main__':
    import os
    import pickle
    wordcounts = {}
    apcount = collections.defaultdict(int)
    
    
    dump_filename_list = os.listdir("feeds_dump")
    for filename in dump_filename_list:
        with open("feeds_dump/%s"%filename, "r") as f:
            feeds = pickle.load(f)
            for url, feed in feeds:
                title, wc = GetWordCount(feed)
                
                if title == None:
                    continue
                
                wordcounts["%s:%s"%(filename, title)] = wc
                for word, count in wc.items():
                    if count >= 1:
                        apcount[word] += 1
                        
    curious_words = []
    for word in apcount:
        occur_rate = float(apcount[word]) / len(wordcounts)
        assert occur_rate <= 1.0
        if 0.1 < occur_rate < 0.5:
            curious_words.append(word)
            
    with open("blogdata.txt", "w") as out:
        out.write("Blog\t%s\n"%"\t".join(curious_words))
        for title, wc in wordcounts.items():
            outlist = [str(wc[word]) for word in curious_words]
            out.write("%s\t%s\n"%(title.encode('utf-8'), "\t".join(outlist)))
        