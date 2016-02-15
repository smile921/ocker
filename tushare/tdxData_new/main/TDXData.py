#_*_coding:utf-8_*_
'''
Created on 2015年5月30日

@author: Administrator
'''

import os
import logging
from datetime import date

import numpy as np
from tool import Connection

class StockData():
    '''通过通达信导出的txt文件，更新日线和分钟级数据'''
    def __init__(self):
        self.initialize()
        self.main()
        
    def initialize(self):
        # 初始化日志服务
        self.initLogging()
        # 初始化路径值
        self.initPath()
        # 数据库连接，连接配置可在tool文件中修改
        self.con = Connection().getConnection()
        self.cur = self.con.cursor()
       
    def main(self):
        # 读入历史数据，只是第一次导入数据库时需要运行，以后每天更新即可。
        self.historyData()
#         self.currentData() # 更新數據庫的數據，每天运行
        self.close()

#------------------------------------------------------------------------------ initialize
    def initLogging(self):
        '''初始化日志对象'''
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)-15s %(lineno)-6d %(funcName)-30s %(message)s',
                            datefmt='%m-%d %H:%M:%S',
                            filename=r'D:\tdxData.txt',
                            filemode='w')
        
    def initPath(self):
        '''初始化各种读入文件的路径名，包括历史的日数据，分钟数据路径；当天的收盘价和前收的对比数据路径，以及当天的日数据和分钟数据的路径名'''
        self.historyDayPath = r'C:\Users\Administrator\Desktop\tdx\historyDayData' #历史日数据文件的路径，会把当前路径的数据全部写入数据库
        self.historyMinPath = r'C:\Users\Administrator\Desktop\tdx\historyMinData' #历史分钟数据文件的路径，会把当前路径的数据全部写入数据库，并相应生成15,30,60数据
        self.checkDataPath  = r'C:\Users\Administrator\Desktop\tdx\checkData' #收盘价和前收的对比文件路径，也就是通过公式导出的文件的路径，该路径只应该存在最新的对比文件
        self.currentMinPath = r'C:\Users\Administrator\Desktop\tdx\currentMinData' #当天分钟级的数据，并相应形成15,30,60数据
        
#------------------------------------------------------------------------------ history
    def historyData(self):
        '''读入历史数据'''
#         self.getDayData()
        self.getMinData()
        
    def getDayData(self):
        '''读入日线数据'''
        # 得到文件夹下面所有txt结尾的文件路径
        fileNameList = self.getFileNameList(self.historyDayPath) # 内容是文件名，不包括前面的路径
        filePathList = [os.path.join(self.historyDayPath,fileName) for fileName in fileNameList]
        symbolList   = [fileName.split('.')[0] for fileName in fileNameList]
        # 循环每个文件写入数据库
        for i in range(len(fileNameList)):
            stockFile = open(filePathList[i])
            fileContent = stockFile.readlines()[:-1] 
            stockFile.close()
            if len(fileContent) == 0:   continue
            # 读取数据
            stockDataList = [] 
            for row in fileContent:
                rowData = row.strip().split('\t')
                valueData = np.array([float(item) for item in rowData[1:]])
                if all(valueData==0.0): continue # 全部为0.证明该股票还未上市操作
                if len(rowData) == 7: # 查看了海信在6-15日停牌的情况，停牌的数据没有包含在下载的文件中。
                    rowData.insert(0,symbolList[i])
                    stockDataList.append(rowData)
            # 将这只股票的数据写入数据库
            sql = "insert ignore into stockData_day_tdx values("+"%s,"*7+"%s)"   # 一共有8列数据
            self.cur.executemany(sql,tuple(stockDataList))
            self.con.commit()
            print '%s日线数据写入完毕' % symbolList[i]
            
        print '全部日线数据写入完毕'
            
    def getMinData(self):
        '''读入分钟数据'''
        # 得到文件夹下面所有txt结尾的文件路径
        fileNameList = self.getFileNameList(self.historyMinPath) # 内容是文件名，不包括前面的路径
        filePathList = [os.path.join(self.historyMinPath,fileName) for fileName in fileNameList]
        symbolList   = [fileName.split('.')[0] for fileName in fileNameList]
        # 循环每个文件写入数据库
        for i in range(len(fileNameList)):
            # 读取文件,从第一行读取（不导出头文件）
            stockFile = open(filePathList[i])
            fileContent = stockFile.readlines()[:-1] 
            stockFile.close()
            if len(fileContent) == 0:   continue
            # 读取数据
            stockDataList5 = [] 
            for row in fileContent:
                rowData = row.strip().split('\t')
                stockTime = '%s %s:%s:00' % (rowData[0],rowData[1][:-2],rowData[1][-2:])
                rowData[0:2] = [stockTime]
                valueData = np.array([float(item) for item in rowData[1:]])
                if all(valueData==0.0): continue # 全部为0.证明该股票还未上市操作
                if len(rowData) == 7: # 查看了海信在6-15日停牌的情况，停牌的数据没有包含在下载的min文件中。
                    rowData.insert(0,symbolList[i])
                    stockDataList5.append(rowData)
            # 找到其他周期的数据
            stockDataList15 = stockDataList5[2::3]
            stockDataList30 = stockDataList15[1::2]
            stockDataList60 = [item for item in stockDataList30 if item[1].endswith('00:00')]
            # 将这只股票的数据写入数据库
            for period in [5,15,30,60]:
                sql = "insert ignore into stockData_%smin_tdx" % period + " values("+"%s,"*7+"%s)"
                self.cur.executemany(sql,tuple(eval("stockDataList%s" % period)))
                self.con.commit()
                print '%s分钟线数据写入完毕' % symbolList[i]
                
        print '全部分钟线数据写入完毕' 
                
#------------------------------------------------------------------------------current
    def currentData(self):
        '''复权，更新当天的数据'''
        stockCodeList = self.getStockCode()
        reRightCode,stockDataList = self.getUpdateCode(stockCodeList)
        self.reRightData(reRightCode) # 复权数据
        self.insertDayData(stockDataList) # 插入今天日线数据
        self.insertMinData() # 插入今天分钟线数据
        
    def getUpdateCode(self,stockCodeList):
        '''獲得不同情況下更新的股票代碼'''
        reRightCode = [] # 需要復權的股票和复权因子的列表
        # 得到数据库中最新一天的收盘价
        closeDict = {} # 股票代码：收盘价
        for code in stockCodeList:
            sql = "select ClosePrice from stockdata_day_tdx where Symbol = %s order by date desc limit 1" 
            self.cur.execute(sql,code)
            result = self.cur.fetchone()
            closeDict[code] = result[0]
        # 查找今天的數據
        fileName = self.getFileNameList(self.checkDataPath)[0]
        path = r'%s/%s' % (self.checkDataPath,fileName)
        
        stockFile = open(path)
        fileContent = stockFile.readlines()[2:-1] # 去掉最后一行
        stockFile.close()
        rowDataList = []
        for row in fileContent:
            rowData = [item.strip() for item in row.strip().split('\t')]
            if len(rowData) != 11 or (rowData[3] == '0') or (rowData[4] == '0'): continue# 不等于11列则该股票还未上市，rowData[3]为空证明停牌，停牌不用复权
            rowData[0] = rowData[0].replace('=','')
            rowData[0] = rowData[0].replace('"','')
            rowDataList.append(rowData)
        # 由文件数据转化成结构数据
        print len(rowDataList)
        stockDataList = self.formatStockData(rowDataList)
        for stockData in stockDataList:
            symbol = stockData[0]
            if symbol not in closeDict: continue
            
            closePrice = closeDict[symbol] # 得到数据库中最新的收盘价
            if closePrice != float(stockData[-1]): # 如果不一致则需要复权
                rate = (float(stockData[-1]) - closePrice) / closePrice
                reRightCode.append([symbol,rate])
                
        print stockDataList
        print '复权的数量为 %s' % len(reRightCode)
        print '复权的股票代码为：'
        print reRightCode
        logging.info('需要复权的股票代码为：%s' % reRightCode)
        return reRightCode,stockDataList
    
    def formatStockData(self,rowDataList):
        '''将从文件读入的数据变为数据库标准数据，代码加前缀，成交量和成交额改变;o,h,l,c,v,a'''
        today = date.today()
        stockDataList = []
        for rowData in rowDataList:
            rowData[0] = 'SH'+rowData[0] if rowData[0].startswith('6') else 'SZ'+rowData[0]
            for i in [4,5]:
                if rowData[i].endswith('\xd2\xda'):
                    rowData[i] = float(rowData[i].replace('\xd2\xda','')) * (10**8)
                elif rowData[i].endswith('\xcd\xf2'):
                    rowData[i] = float(rowData[i].replace('\xcd\xf2','')) * (10**4)
            stockData = [rowData[0],today,rowData[9],rowData[7],rowData[8],rowData[6],rowData[4],rowData[5],rowData[10]]      
            stockDataList.append(stockData)
        return stockDataList
    
    def reRightData(self,reRightCode):
        ''''''
        code = [record[0] for record in reRightCode]
        rate = [record[1] for record in reRightCode]
        
        paraList = []
        for i in range(len(reRightCode)):
            paraList.append((rate[i],)*5 + (code[i],))
             
        sql = 'update stockData_day_tdx set '+\
        'OpenPrice=OpenPrice*(1+%s),HighPrice=HighPrice*(1+%s),LowPrice=LowPrice*(1+%s),ClosePrice=ClosePrice*(1+%s),Amt=Amt*(1+%s)' +\
         " where Symbol=%s" 
        self.cur.executemany(sql,paraList)
        self.con.commit()
        print '日线复权完毕'
        paraList = []
        for i in range(len(reRightCode)):
            paraList.append((rate[i],)*5 + (code[i],))
            
        for period in [5,15,30,60]:
            sql = 'update stockData_%smin_tdx ' % period + \
            'set OpenPrice=OpenPrice*(1+%s),HighPrice=HighPrice*(1+%s),LowPrice=LowPrice*(1+%s),ClosePrice=ClosePrice*(1+%s),Amt=Amt*(1+%s)' + \
            " where Symbol=%s" 
            self.cur.executemany(sql,paraList)
            self.con.commit()
        print '分钟线复权完毕'
    
    def insertDayData(self,todayData):
        '''更新日数据'''
        for data in todayData:
            data[:] = data[:-1]
        sql = "insert ignore into stockData_day_tdx values("+"%s,"*7+"%s)"   # 一共有8列数据
        self.cur.executemany(sql,tuple(todayData))
        self.con.commit()
        print 'day数据更新完成'
        
    def insertMinData(self):
        '''更新分钟数据'''
        today = date.today().strftime('%Y-%m-%d')
        # 得到文件夹下面所有txt结尾的文件路径
        fileNameList = self.getFileNameList(self.currentMinPath) # 内容是文件名，不包括前面的路径
        filePathList = [os.path.join(self.historyDayPath,fileName) for fileName in fileNameList]
        symbolList   = [fileName.split('.')[0] for fileName in fileNameList]
        # 循环每个文件写入数据库
        for i in range(len(fileNameList)):
            # 读取文件,从第一行读取（不导出头文件）
            stockFile = open(filePathList[i])
            fileContent = stockFile.readlines()[:-1] 
            stockFile.close()
            if len(fileContent) == 0:   continue
            
            stockDataList5 = [] 
            for row in fileContent[::-1]:
                rowData = row.strip().split('\t')
                if rowData[0] != today: break # 从最后一行读取，如果其日期不是当天日期，则退出循环。
                stockTime = '%s %s:%s:00' % (rowData[0],rowData[1][:-2],rowData[1][-2:])
                rowData[0:2] = [stockTime]
                valueData = np.array([float(item) for item in rowData[1:]])
                if all(valueData==0.0): continue # 全部为0.证明该股票还未上市操作
                if len(rowData) == 7: # 查看了海信在6-15日停牌的情况，停牌的数据没有包含在下载的min文件中。
                    rowData.insert(0,symbolList[i])
                    stockDataList5.append(rowData)
            # 找到其他周期的数据
            stockDataList15 = stockDataList5[2::3]
            stockDataList30 = stockDataList15[1::2]
            stockDataList60 = [item for item in stockDataList30 if item[1].endswith('00:00')]
            # 将这只股票的数据写入数据库
            for period in [5,15,30,60]:
                sql = "insert ignore into stockData_%smin_tdx" % period + " values("+"%s,"*7+"%s)"
                self.cur.executemany(sql,tuple(eval("stockDataList%s" % period)))
                self.con.commit()
                print '%s分钟线数据写入完毕' % symbolList[i]
        print 'min数据更新完成'
    
    def getFileNameList(self,filePath):
        '''得到路径下所有的文件名'''
        return [filename for filename in os.listdir(filePath) if os.path.splitext(filename)[-1] in ['.xls','.xlsx']]
        
    def getStockCode(self):
        '''获得股票代码'''
        sql = 'select distinct Symbol from stockdata_day_tdx'
        self.cur.execute(sql)
        return [symbol[0] for symbol in self.cur.fetchall() if symbol[0]]
        
    def close(self):
        '''關閉數據庫'''
        self.con.close()
        self.cur.close()
  
if __name__ == '__main__':
    
    stockData = StockData()
