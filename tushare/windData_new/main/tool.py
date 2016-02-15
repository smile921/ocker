# *_* encoding=utf-8*_*
'''
Created on 2015��4��28��

@author: Administrator
'''

import MySQLdb

class Connection(object):
    
    @staticmethod
    def getConnection(localhost='localhost', user='root', passwd='', db='hangqing', charset='utf8'):
        try:
            con = MySQLdb.connect(host=localhost, user=user, passwd=passwd, db=db, charset=charset)
            return con
        except:
            return None
        
