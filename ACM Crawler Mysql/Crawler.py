import urllib.request
import time
import random
import os
import os.path
import Parse
import subprocess

def getTime():
    time_format = '%Y-%m-%d %X'
    return time.strftime(time_format, time.localtime(time.time()))

class Crawler():
    lastPath = ''

    def netTest(self, testUrl):
        p = subprocess.Popen(['ping.exe', testUrl],  stdin = subprocess.PIPE, 
                                         stdout = subprocess.PIPE, 
                                         stderr = subprocess.PIPE, 
                                         shell = True)
        out = p.stdout.read()
        if('Request timed out.' in out):
            print(getTime + ' Net ERR... Sleep 1h')
            time.sleep(3600)
    
    def crawlURL(self, url):
        time.sleep(random.randint(1,5))
        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'}
        req = urllib.request.Request(url, headers=headers)
        content = urllib.request.urlopen(req).read()
        self.saveUrl(url, content, 'HTML_Data')
        return content

    def saveUrl(self, url, content, root_path):
        url = url.replace('http://','').replace('/','#').replace('?','$')
        d_list = os.listdir(root_path)
        n_dir = len(d_list) - 1
        if(n_dir == -1):
            n_dir = 0
            os.mkdir(root_path + '\\' + str(n_dir))
        dir_path = root_path + '\\' + str(n_dir)
        f_list = os.listdir(dir_path)
        n_file = len(f_list)
        if(n_file > 9999):
            dir_path = root_path + '\\' + str(n_dir + 1)
            os.mkdir(dir_path)

        file_path = dir_path + '\\' + url + '.html'
        file = open(file_path, 'wb')
        file.write(content)
        file.close()
        self.lastPath = file_path

    def crawlAuthorPub(self, userId):
        url = 'http://dl.acm.org/authorBibTex.cfm?query=%28Author%3A' + userId + '%29'
        return self.crawlURL(url)

    def crawlPaperMain(self, PaperId):
        url = 'http://dl.acm.org/citation.cfm?id=' + PaperId + '&preflayout=flat'
        return self.crawlURL(url)

    def crawlPaperAbstract(self, PaperId):
        url = 'http://dl.acm.org/tab_abstract.cfm?id=' + PaperId + '&usebody=tabbody'
        return self.crawlURL(url)

    def crawlPaperRef(self, PaperId):
        url = 'http://dl.acm.org/tab_references.cfm?id=' + PaperId + '&usebody=tabbody'
        return self.crawlURL(url)

    def crawlPaperIndex(self, PaperId):
        url = 'http://dl.acm.org/tab_indexterms.cfm?id=' + PaperId + '&usebody=tabbody'
        return self.crawlURL(url)

    def crawlPaperCiting(self, PaperId):
        url = 'http://dl.acm.org/tab_citings.cfm?id=' + PaperId + '&usebody=tabbody'
        return self.crawlURL(url)

    def crawlPaperList(self, PaperId):
        url = 'http://dl.acm.org/tab_about.cfm?id=' + PaperId + '''&type=article&parent_id=2488388&parent_type=proceeding&title=Personalized%20recommendation%20via%20cross%2Ddomain%20triadic%20factorization&toctitle=Proceedings%20of%20the%2022nd%20international%20conference%20on%20World%20Wide%20Web&tocissue_date=&notoc=0&usebody=tabbody&tocnext_id=&tocnext_str=&tocprev_id=2487788&tocprev_str=WWW '13 Companion&toctype=proceeding&cfid=421040626&cftoken=61912098&_cf_containerId=prox&_cf_nodebug=true&_cf_nocache=true&_cf_clientid=B5CB992AF8CE6CC699619AD53D81A429&_cf_rc=8'''
        return self.crawlURL(url)


#print(Crawler.crawlPaperMain(Crawler(), '2488441').decode('utf-8'))
#cr = Crawler()
#cr.crawlPaperMain('2488441')
#pr = Parse.Parse()
#content = cr.crawlURL('http://dl.acm.org/authorBibTex.cfm?query=%28Author%3A81758651657%29')
#pr.parseAuthorPub(content)

