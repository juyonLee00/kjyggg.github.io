from crawLibWadiz import *

pagename ='reward'
page_url = 'https://www.wadiz.kr/web/wreward/main?keyword=&endYn=ALL&order=recent'
nUrl = 48
wc = WadizCrawler()
wc.getUrlLister(pagename, page_url, nUrl)

