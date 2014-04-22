import Crawler
import Parse
from bs4 import BeautifulSoup
import SQLConn

def Init(pr, sql):
    pr.sql = sql
    print('init finished...')

def outPut_sql(paper):
    qa_sql = SQLConn.QASQL()
    if(qa_sql.checkPaper(paper.id) == False):
        qa_sql.InsertPaper(paper)

def processPaper(paperId):
    content = cr.crawlPaperMain(paperId)
    soup = BeautifulSoup(content)
    paper = pr.getPaperInfo(soup)
    paper.id = paperId
    outPut_sql(paper)

def processUser(userId):
    content = cr.crawlAuthorPub(userId)
    pr.parseAuthorPub(content)
    
cr = Crawler.Crawler()
pr = Parse.Parse()
sql = SQLConn.MysqlUti()

Init(pr, sql)
pr.testPaper('2488441')

while((sql.getPaper()!=None) or (sql.getUser()!=None)):
    paperId = sql.getPaper()
    while paperId != None:
        print(paperId)
        try:
            processPaper(paperId)
            sql.updatePaper(paperId, 1)
        except Exception:
            sql.insertErr('Paper Err', paperId, cr.lastPath)
            sql.updatePaper(paperId, -1)
        paperId = sql.getPaper()

    userId = sql.getUser()
    print('Process User' + userId)
    try:
        processUser(userId)
        sql.updateUser(userId, 1)
    except Exception:
        sql.insertErr('User Err', userId, cr.lastPath)
        sql.updateUser(userId, -1)
print('all empty')
        



