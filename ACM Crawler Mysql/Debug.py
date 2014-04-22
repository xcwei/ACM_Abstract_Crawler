import Crawler
import Parse
from bs4 import BeautifulSoup
import SQLConn

def outPut_sql(paper):
    qa_sql = SQLConn.QASQL()
    if(qa_sql.checkPaper(paper.id) == False):
        qa_sql.InsertPaper(paper)

pr = Parse.Parse()
sql = SQLConn.MysqlUti()
pr.sql = sql
file = open('HTML_Data/0/dl.acm.org#citation.cfm$id=1134732&preflayout=flat.html', 'rb')
soup = BeautifulSoup(file)
paper = pr.getPaperInfo(soup)
paper.id = '1134732'
outPut_sql(paper)
