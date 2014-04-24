from bs4 import BeautifulSoup
import re
import SQLConn

class Parse():
    userId = {}
    arr_user = []
    idx_user = 0
    paperId = {}
    arr_paper = []
    idx_paper = 0
    pubId = {}
    sql = None

    def testUser(self, uid):        
        if self.sql.checkUser(uid)==False:
            self.sql.insertUser(uid, 0)



    def testPaper(self, pid):
        if self.sql.checkPaper(pid)==False:
            self.sql.insertPaper(pid, 0)

    
    def getAuthor(self, soup):
        arr_authors = []
        s_authors = soup.find('table', class_='medium-text').find_all("a", title='Author Profile Page')
        s_inst = soup.find('table', class_='medium-text').find_all("a", title='Institutional Profile Page')
        if(len(s_authors) == len(s_inst)):
            for i in range(0, len(s_authors)):
                author = Author()
                author.name = s_authors[i].get_text()
                author.inst = s_inst[i].get_text()
                author.id = re.findall('(?<=cfm\?id\=)[^&]+', s_authors[i].get('href'))[0]
                self.testUser(author.id)
                arr_authors.append(author)
#                print(author.name)
        else:
            for i in range(0, len(s_authors)):
                author = Author()
                author.name = s_authors[i].get_text()
                author.id = re.findall('(?<=cfm\?id\=)[^&]+', s_authors[i].get('href'))[0]
                self.testUser(author.id)
                arr_authors.append(author)
        return arr_authors
                
    def getPaperInfo(self, soup):
        paper = Paper()
        paper.title = soup.find('div', class_='large-text').get_text().strip()
#        print(paper.title)
        paper.author = self.getAuthor(soup)


        
        s_layout = soup.find('div', class_='layout')
        s_abstract = s_layout.find('div', style='display:inline')
        if(s_abstract != None):
            paper.abstract = s_abstract.get_text().replace('\r', '')
        else:
            paper.abstract = ''
#        print(paper.abstract)

        s_flat = soup.find_all('div', style='margin-left:10px; margin-top:0px; margin-right:10px; margin-bottom: 10px;', class_='flatbody')
        if(len(s_flat) == 6):
            s_flat.append(s_flat[5])
            s_flat[5] = s_flat[4]
            s_flat[4] = s_flat[3]
            s_flat[3] = s_flat[2]
            s_flat[2] = s_flat[1]
            s_flat[1] = s_flat[0]
        #reference:
        s_refs = s_flat[1].find_all('tr')
        for s_ref in s_refs:
            s_p = s_ref.find('div', class_='')
            p = s_p.get_text().strip()

            s_pId = s_p.find('a')
            if(s_pId != None):
                pId = re.findall('(?<=cfm\?id\=)[^&]+', s_pId.get('href'))
                if(len(pId)>0):
                    p += '\t' + pId[0]
                    self.testPaper(pId[0])
#            print(p + '\n')
            paper.ref.append(p)

        #citation:
        s_cites = s_flat[2].find_all('div', class_='')
        for s_cite in s_cites:
            c = s_cite.get_text().strip()

            s_cId = s_cite.find('a')
            if(s_cId != None):
                cId = re.findall('(?<=cfm\?id\=)[^&]+', s_cId.get('href'))
                if(len(cId)>0):
                    c += '\t' + cId[0]
                    self.testPaper(cId[0])
#            print(p + '\n')
            paper.cit.append(c)

        #index:
        s_indexes = s_flat[3].find_all('a', target='_self')
        for s_index in s_indexes:
            paper.index.append(s_index.get_text())
#        print(paper.index)

        #pub
        s_pub = s_flat[4]
        s_p = s_pub.find('tr', valign='top')
        pub = s_p.get_text()
        pub = pub.replace('Title', '').replace('table of contents', '').replace('archive', '').strip()
        s_pubId = s_p.find('a', text='table of contents')
        if(s_pubId != None):
            pubId = re.findall('(?<=cfm\?id\=)[^&]+', s_pubId.get('href'))
            if(len(pubId)>0):
                pub += '\t' + pubId[0]
        paper.pub = pub

        s_content = soup.find('table', class_='text12', border='0')
        if(s_content == None):
            return paper
        items = s_content.find_all('a')
        for item in items:
            url = item.get('href')
            if('author_page' in url):
                userId = re.findall('(?<=cfm\?id\=)[^&]+', url)
                self.testUser(userId[0])
            elif('citation' in url):
                paperId = re.findall('(?<=cfm\?id\=)[^&]+', url)
                self.testPaper(paperId[0])
        return paper
        
    def parseAuthorPub(self, content):
        paperIds = re.findall('(?<=@inproceedings\{)\d+', content.decode('utf-8'))
        for item in paperIds:
            self.testPaper(item)
        paperIds = re.findall('(?<=@article\{)\d+', content.decode('utf-8'))
        for item in paperIds:
            self.testPaper(item)



class Author():
    def __init__(self):
        self.name = ''
        self.id = ''
        self.inst = ''

class Paper():
    def __init__(self):
        self.id = ''
        self.title = ''
        self.abstract = ''
        self.author = []
        self.ref = []
        self.cit = []
        self.index = []
        self.pub = ''
        
