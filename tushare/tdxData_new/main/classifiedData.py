#_*_coding:utf-8_*_
'''
Created on 2015年7月20日

@author: Administrator
'''

import logging
import datetime
import collections

import numpy as np
import pandas as pd

from tool import Connection

class ClassifiedData(object):
    '''新浪行业分类指数的构造'''
    def __init__(self):
        self.initialize()
        self.main()
        
    def initialize(self):
        # 初始化日志服务
        self.initLogging()
        # 数据库连接，连接配置可在tool文件中修改
        self.con = Connection().getConnection()
        self.cur = self.con.cursor()
        
    def main(self):
        classifiedDict = self.getClassifiedDict()
        for key in classifiedDict:
            self.createClassifiedData(key,classifiedDict[key])
            
#------------------------------------------------------------------------------ 
    def initLogging(self):
        '''初始化日志对象'''
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)-15s %(lineno)-6d %(funcName)-30s %(message)s',
                            datefmt='%m-%d %H:%M:%S',
                            filename=r'D:\classifiedData.txt',
                            filemode='w')
        
    def getClassifiedDict(self):
        
        classifiedDict = collections.defaultdict(list)
        
        sql = 'select symbol,industryname from industryclassified'
        self.cur.execute(sql)
        for record in self.cur.fetchall():
            symbol = u'SH' + record[0] if record[0][0] in ['5','6'] else u'SZ' + record[0]
            classifiedDict[record[1]].append(symbol)
            
        return classifiedDict
    
    def createClassifiedData(self,classifiedName,symbolList):
        '''生成行业的指数，从2007-01-01开始'''
        stockDataList = []
        firstDataList = []
        insertDataList = []
        
        firstDay = datetime.date(2007,1,4)
        sql = 'select date,openprice,highprice,lowprice,closeprice from stockdata_day_tdx where symbol = "%s" and date >= "2007-01-01" order by date'
        for symbol in symbolList:
            stockData = []
            self.cur.execute(sql % symbol)
            for record in self.cur.fetchall():
                stockData.append(list(record))
            if not stockData:   continue
            
            stockData = np.array(stockData)
            stockDataDict = {'openprice':stockData[:,1],'highprice':stockData[:,2],'lowprice':stockData[:,3],
                             'closeprice':stockData[:,4]}
            stockDataFrame = pd.DataFrame(stockDataDict,index=stockData[:,0],columns=['openprice','highprice','lowprice',
                                                                                     'closeprice'])
            if firstDay in stockDataFrame.index:
                firstDataList.append(stockDataFrame.ix[firstDay].values)
            # 存放该行业的所有的股票数据
            stockDataFrame = (stockDataFrame / stockDataFrame.shift(1) - 1).dropna()
            stockDataList.append(stockDataFrame)
        # 获取第一天的数据   
        firstDataList = np.array(firstDataList)
        firstOpen = 1000
        firstHigh = round(firstDataList[:,1].sum() / firstDataList[:,0].sum() * 1000,2)
        firstLow  = round(firstDataList[:,2].sum() / firstDataList[:,0].sum() * 1000,2)
        firstClose = round(firstDataList[:,3].sum() / firstDataList[:,0].sum() * 1000,2)
        insertDataList.append([classifiedName,firstDay,firstOpen,firstHigh,firstLow,firstClose])
        # 获得其它日期的数据
        dateList = [dataDate.to_datetime().date() for dataDate in pd.date_range('2007-01-04',datetime.date.today(),freq='B')]
        dataDict = {dataDate:[stockDataFrame.ix[dataDate].values for stockDataFrame in stockDataList if dataDate in stockDataFrame.index] for dataDate in dateList}
        
        for key in sorted(dataDict.keys()):
            yesterdayData = insertDataList[-1]
            if not dataDict[key]:    continue
            otherDayData = np.array(dataDict[key])
            otherOpen = round(yesterdayData[2] * (1 + otherDayData[:,0].mean()),2)
            otherHigh = round(yesterdayData[3] * (1 + otherDayData[:,1].mean()),2)
            otherLow  = round(yesterdayData[4] * (1 + otherDayData[:,2].mean()),2)
            otherClose = round(yesterdayData[5] * (1 + otherDayData[:,3].mean()),2)
            insertDataList.append([classifiedName,key,otherOpen,otherHigh,otherLow,otherClose])
        
        sql = 'insert ignore into classifieddata value(' + '%s,' * 5 + '%s)'
        self.cur.executemany(sql,tuple(insertDataList))
        self.con.commit()
        
        
if __name__ == '__main__':
    
    classifiedData = ClassifiedData()
        