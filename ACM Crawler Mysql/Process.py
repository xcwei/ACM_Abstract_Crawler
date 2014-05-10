# author: Xiaochi Wei
# Email: wxchi@bit.edu.cn
# Date: May 10, 2014

import Crawler
import Parse
from bs4 import BeautifulSoup
import SQLConn
import time
import datetime

def checkTime():
    if(datetime.datetime.now().weekday()>4):
        return False
    
    time_format = '%X'
    t = time.strftime(time_format, time.localtime(time.time()))

    arr_t = t.split(':')
    int_t = int(arr_t[0])*3600 + int(arr_t[1])*60
    int_start = 9*3600
    int_stop = 24*3600
    if(int_t > int_stop or int_t < int_start):
        return False
    else:
        return True

def Init(pr, sql):
    pr.sql = sql
    print('init finished...')

def outPut_sql(paper):
    qa_sql = SQLConn.QASQL()
    qa_sql.Connect()
    if(qa_sql.checkPaper(paper.id) == False):
        qa_sql.InsertPaper(paper)
    qa_sql.Disconnect()

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
paperId = sql.getPaper()


while((paperId!=None) or (sql.getUser()!=None)):
    while paperId != None:
        while(checkTime() == False):
            print('sleeping... ')
            time.sleep(1.5 * 3600)
            
        print(paperId)
        try:
            processPaper(paperId)
            sql.updatePaper(paperId, 1)
        except Exception:
            sql.insertErr('Paper Err', paperId, cr.lastPath)
            sql.updatePaper(paperId, -1)
            
        try:
            paperId = sql.getPaper()
        except:
            print('sql err')

    userId = sql.getUser()
    print('Process User' + userId)
    try:
        processUser(userId)
        sql.updateUser(userId, 1)
    except Exception:
        sql.insertErr('User Err', userId, cr.lastPath)
        sql.updateUser(userId, -1)
print('all empty')
        



