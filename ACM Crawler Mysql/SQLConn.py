import mysql.connector
import time

class QASQL():

    user = 'root'
#    pwd = 'autoqa'
#    host = '10.108.17.25'
    db = 'autoqa'

#    user = 'root'
    pwd = '30291912'
    host = 'localhost'
#    db = 'autoqa'
    conn = None
    cursor = None

    def Connect(self):
        self.conn = mysql.connector.connect(user=self.user, password=self.pwd, host=self.host, database=self.db)
        self.cursor = self.conn.cursor()
        
    def Disconnect(self):
        self.conn.close()
        self.cursor.close()

    def checkPaper(self, id):
        sel_sql = 'SELECT * FROM paper WHERE Id=\"{}\"'.format(id)
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('paper sel err')
        if(len(self.cursor.fetchall())>0):
            return True
        else:
            return False

    def checkAuthor(self, id):
        sel_sql = 'SELECT * FROM author WHERE Id=\"{}\"'.format(id)
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('author sel err')
        if(len(self.cursor.fetchall())>0):
            return True
        else:
            return False

    def checkPub(self, id):
        sel_sql = 'SELECT * FROM pub WHERE Id=\"{}\"'.format(id)
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('pub sel err')
        if(len(self.cursor.fetchall())>0):
            return True
        else:
            return False

    def checkIdx(self, idx):
        sel_sql = 'SELECT * FROM idx WHERE NAME=\"{}\"'.format(idx.replace('\"', '\\\"'))
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('idx sel err')
        if(len(self.cursor.fetchall())>0):
            return True
        else:
            return False

    def getIdxId(self, idx):
        sel_sql = 'SELECT * FROM idx WHERE NAME=\"{}\"'.format(idx.replace('\"', '\\\"'))
        id = ''
        try:
            self.cursor.execute(sel_sql)
            id = self.cursor.fetchall()[0][0]
        except:
            print(sel_sql)
            print('idx sel err')       
        return id

    def InsertPaper(self, paper):
        arr_pub = paper.pub.split('\t')
        pubId = ''
        pubInfo = ''
        if(len(arr_pub)>1):
            pubId = arr_pub[1]
            pubInfo = arr_pub[0]
            if(self.checkPub(pubId) == False):
                insert_sql_pub = 'INSERT INTO pub (ID, NAME) VALUES (\"{}\", \"{}\")'.format(pubId, pubInfo.replace('\"', '\\\"'))
                try:
                    self.cursor.execute(insert_sql_pub)
                    self.conn.commit()
                except:
                    print(insert_sql_pub)
                    print('pub insert err')
        insert_sql1 = 'INSERT INTO paper (Id, TITLE, ABSTRACT, PUB) VALUES (\"{}\", \"{}\", \"{}\", \"{}\")'.format(paper.id, paper.title.replace('\"', '\\\"'), paper.abstract.replace('\"', '\\\"'), pubId)
        try:
            self.cursor.execute(insert_sql1)
            self.conn.commit()
        except:
            print(insert_sql1)
            print('peper insert err')
        author_rank = 0
        for item in paper.author:
            author_rank += 1
            if(self.checkAuthor(item.id) == False):
                insert_sql2 = 'INSERT INTO author (ID, NAME, INFO) VALUES (\"{}\", \"{}\", \"{}\")'.format(item.id, item.name.replace('\"', '\\\"'), item.inst.replace('\"', '\\\"'))
                try:
                    self.cursor.execute(insert_sql2)
#                    self.conn.commit()
                except:
                    print(insert_sql2)
                    print('author insert err')
            insert_sql3 = 'INSERT INTO ref_author_paper (AUTHOR_ID, PAPER_ID, RANK) VALUES (\"{}\", \"{}\", {})'.format(item.id, paper.id, author_rank)
            try:
                self.cursor.execute(insert_sql3)
#                self.conn.commit()
            except:
                print(insert_sql3)
                print('ref_author_paper insert err')
        for item in paper.ref:
            arr_ref = item.split('\t')
            if(len(arr_ref)>1):
                source_id = paper.id
                ref_id = arr_ref[1]
                insert_sql4 = 'INSERT INTO ref_paper (SOURCE_PAPER, REF_PAPER) VALUES (\"{}\", \"{}\")'.format(source_id, ref_id)
                try:
                    self.cursor.execute(insert_sql4)
#                    self.conn.commit()
                except:
                    print(insert_sql4)
                    print('ref_paper insert err')
        for item in paper.cit:
            arr_cit = item.split('\t')
            if(len(arr_cit)>1):
                ref_id = paper.id
                source_id = arr_cit[1]
                insert_sql5 = 'INSERT INTO ref_paper (SOURCE_PAPER, REF_PAPER) VALUES (\"{}\", \"{}\")'.format(source_id, ref_id)
                try:
                    self.cursor.execute(insert_sql5)
#                    self.conn.commit()
                except:
                    print(insert_sql5)
                    print('ref_paper insert err')
        self.conn.commit()
        for item in paper.index:
            if(self.checkIdx(item) == False):
                insert_sql6 = 'INSERT INTO idx (NAME) VALUES (\"{}\")'.format(item.replace('\"', '\\\"'))
                try:
                    self.cursor.execute(insert_sql6)
                    self.conn.commit()
                except:
                    print(insert_sql6)
                    print('idx insert err')
            insert_sql7 = 'INSERT INTO ref_paper_idx (PAPER_ID, IDX_ID) VALUES (\"{}\", \"{}\")'.format(paper.id, self.getIdxId(item))
            try:
                self.cursor.execute(insert_sql7)
                self.conn.commit()
            except:
                print(insert_sql7)
                print('idx insert err')



class MysqlUti():
    
    user = 'root'
#    pwd = 'autoqa'
#    host = '10.108.17.25'
    db = 'ACM_Crawler'

#    user = 'root'
    pwd = '30291912'
    host = 'localhost'
#    db = 'ACM_Crawler'

    
    conn = None
    cursor = None

    def Connect(self):
        self.conn = mysql.connector.connect(user=self.user, password=self.pwd, host=self.host, database=self.db)
        self.cursor = self.conn.cursor()

    def Disconnect(self):
        self.conn.close()
        self.cursor.close()

    def getTime(self):
        time_format = '%Y-%m-%d %X'
        return time.strftime(time_format, time.localtime(time.time()))

    def checkUser(self,id):
        self.Connect()
        sel_sql = 'SELECT * FROM user WHERE Id=\'{}\''.format(id)
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('user sel err')
        if(len(self.cursor.fetchall())>0):
            self.Disconnect()
            return True
        else:
            self.Disconnect()
            return False

    def checkPaper(self,id):
        self.Connect()
        sel_sql = 'SELECT * FROM paper WHERE Id=\'{}\''.format(id)
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('paper sel err')
        if(len(self.cursor.fetchall())>0):
            self.Disconnect()
            return True
        else:
            self.Disconnect()
            return False

    def getPaper(self):
        self.Connect()
        sel_sql = 'SELECT Id FROM paper WHERE HasCrawled=0 LIMIT 0,1'     
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('paper sel err')
        item = self.cursor.fetchall()
        self.Disconnect()
        if(len(item) > 0):
            return item[0][0]
        else:
            return None
        
    def getUser(self):
        self.Connect()
        sel_sql = 'SELECT Id FROM user WHERE HasCrawled=0 LIMIT 0,1'
        try:
            self.cursor.execute(sel_sql)
        except:
            print(sel_sql)
            print('user sel err')
        item = self.cursor.fetchall()
        self.Disconnect()
        if(len(item) > 0):
            return item[0][0]
        else:
            return None

    def insertPaper(self,id, hasCr):
        self.Connect()
        insert_sql = 'INSERT INTO paper (Id, HasCrawled) VALUES (\'{}\', {})'.format(id, hasCr)
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except:
            print(insert_sql)
            print('peper insert err')
        self.Disconnect()

    def updatePaper(self,id, hasCr):
        self.Connect()
        update_sql = 'UPDATE paper SET HasCrawled={}, CrawlTime=\'{}\' WHERE Id=\'{}\''.format(hasCr, self.getTime(), id)
        try:
            self.cursor.execute(update_sql)
            self.conn.commit()
        except:
            print(update_sql)
            print('peper update err')
        self.Disconnect()

    def insertUser(self,id, hasCr):
        self.Connect()
        insert_sql = 'INSERT INTO user (Id, HasCrawled) VALUES (\'{}\', {})'.format(id, hasCr)
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except:
            print(insert_sql)
            print('user insert err')
        self.Disconnect()

    def updateUser(self,id, hasCr):
        self.Connect()
        update_sql = 'UPDATE user SET HasCrawled={}, CrawlTime=\'{}\' WHERE Id=\'{}\''.format(hasCr, self.getTime(), id)
        try:
            self.cursor.execute(update_sql)
            self.conn.commit()
        except:
            print(update_sql)
            print('user update err')
        self.Disconnect()

    def insertErr(self,err_type, id, file_path):
        self.Connect()
        insert_sql = 'INSERT INTO err (Id, type, file, ErrTime, HasProcess) VALUES (\'{}\', \'{}\', \'{}\', \'{}\', {})'.format(id, err_type, file_path, self.getTime(), 0)
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except:
            print(insert_sql)
            print('err insert err')
        self.Disconnect()

    def updateErr(self,err_type, id, hasPr):
        self.Connect()
        update_sql = 'UPDATE err SET HasProcess={} WHERE type=\'{}\' AND Id=\'{}\''.format(hasPr, err_type, id)
        try:
            self.cursor.execute(update_sql)
            self.conn.commit()
        except:
            print(update_sql)
            print('err update err')
        self.Disconnect()

