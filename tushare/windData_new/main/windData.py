#_*_coding:utf-8_*_
'''
Created on 2015年6月22日

@author: Administrator
'''

import logging
import datetime
import numpy as np
import pandas as pd
from WindPy import w
from datetime import date
from tool import Connection

class StockData():
    '''获得股票数据'''
    def __init__(self):
        self.initialize()
        self.main()
        
    def initialize(self):
        w.start()
        self.initLogging()
        self.con = Connection().getConnection()
        self.cur = self.con.cursor()
       
       
    def main(self):
        self.historyWindData() # 历史数据，日线和分钟线
        self.updateWindCode()  # 更新A股的代码
        self.currentWindData() # 每日的更新
        self.close()
        
#------------------------------------------------------------------------------ init
    def initLogging(self):
        '''初始化日志对象'''
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)-15s %(lineno)-6d %(funcName)-30s %(message)s',
                            datefmt='%m-%d %H:%M:%S',
                            filename=r'D:\log_windData.txt',
                            filemode='w')
        
#------------------------------------------------------------------------------ historyData
    def historyWindData(self):
        '''得到历史数据'''
        stockCodeList = self.getStockCode() # 股票代码
        fields = {'d':['pre_close','open','high','low','close','volume','amt','pct_chg'],
                  'm':['open','high','low','close','volume','amt','pct_chg']}
        startDate = {'d':'20050101','m':'2014-01-01 09:30:00'}
        endDate   = {'d':datetime.date.today().strftime('%Y%m%d'),'m':datetime.date.today().strftime('%Y-%m-%d')+' 15:00:00'}
        # day
        self.getDayData(stockCodeList,fields['d'],startDate['d'],endDate['d'],option='PriceAdj=F')
        # min
        for period in [5,15,30,60]:
            self.getMinData(stockCodeList,fields['m'],startDate['m'],endDate['m'],period)
        
    def getDayData(self,stockCodeList,fields,startDate,endDate,option):
        '''日数据的具体的获取步骤'''
        for stockCode in stockCodeList:
            print '添加日数据,代碼%s' % stockCode
            wsd_data = w.wsd(stockCode,fields,startDate,endDate,option)
            if wsd_data.ErrorCode==0: # 返回成功
                stockDate = wsd_data.Times
                dateList = [date.strftime("%Y-%m-%d") for date in stockDate]
                stockDataDict = {'stockCode':stockCode,'date':dateList}
                for i in range(len(fields)):
                    stockDataDict[fields[i]] = wsd_data.Data[i]
                stockData = pd.DataFrame(stockDataDict,columns=['stockCode','date']+fields,index=dateList).dropna() # 只要有缺失的数据就删掉这一行，保证数据最为干净
                stockData['pct_chg'] = stockData['pct_chg'] / 100 # 让涨幅变为实际涨幅（查询出来的是百分比数据）
                # 插入到數據庫中
                sql = "insert ignore into stockdata_day values(" + "%s,"*(len(fields)+1)+"%s)" 
                self.cur.executemany(sql,tuple(stockData.values.tolist()))
                self.con.commit()
            else:
                logging.info('ERROR-%s-day數據下載失敗，錯誤代碼為：%s' % (stockCode,wsd_data.ErrorCode))
                
                
    def getMinData(self,stockCodeList,fields,startDate,endDate,period):
        '''分钟线的具体的获取步骤'''
        option = 'BarSize=%s;PriceAdj=F' % period
        for stockCode in stockCodeList:
            print '添加%s 分钟数据,代碼%s' % (period,stockCode)
            wsi_data = w.wsi(stockCode,fields,startDate,endDate,option)
            if wsi_data.ErrorCode==0: # 返回成功
                stockDate = wsi_data.Times
                timeList = [time.strftime("%Y-%m-%d %H-%M-%S") for time in stockDate]
                stockDataDict = {'stockCode':stockCode,'time':timeList}
                for i in range(len(fields)):
                    stockDataDict[fields[i]] = wsi_data.Data[i]
                stockData = pd.DataFrame(stockDataDict,columns=['stockCode','time']+fields,index=timeList).dropna() # 只要有缺失的数据就删掉这一行，保证数据最为干净
                stockData['pct_chg'] = stockData['pct_chg'] / 100 # 让涨幅变为实际涨幅（查询出来的是百分比数据）
                # 插入到數據庫中
                sql = "insert ignore into stockData_%smin" % period + " values("+"%s,"*(len(fields)+1)+"%s)"   
                self.cur.executemany(sql,tuple(stockData.values.tolist()))
                self.con.commit()
            else:
                logging.info('ERROR-%s歷史分鐘%smin數據下載失敗' % (stockCode,period))
    
    
    def updateWindCode(self):
        '''更新股票代码'''
        today = date.today().strftime('%Y%m%d')
        field = 'wind_code,sec_name' # 字段名：股票代码和股票名称
        sector = '全部A股'
        option = 'date=%s;sector=%s;field=%s' % (today,sector,field)
        
        wset_data = w.wset('SectorConstituent',option)
        if wset_data.ErrorCode == 0:
            stockCodeData = zip(wset_data.Data[0],wset_data.Data[1]) # 返回值data[0],data[1]分别为代码和名称
            sql = "delete from stockCode"
            self.cur.execute(sql)
            self.con.commit()
            print '删除代码完毕'
            # 插入到数据库中
            sql = "insert ignore into stockCode values(%s,%s)"
            self.cur.executemany(sql,tuple(stockCodeData))
            self.con.commit()
        else:
            logging.info('ERROR-股票代码更新错误')
        print '更新代码完毕'
        
#------------------------------------------------------------------------------ currentData
    def currentWindData(self):
        '''更新数据'''
        stockCodeList = self.getStockCode()
        reRightCode = self.getUpdateCode(stockCodeList)
        self.reRightData(reRightCode) # 复权数据
        self.insertTodayData(stockCodeList) # 插入今天数据
        
    def getUpdateCode(self,stockCodeList):
        '''得到直接插入和复权的代码'''
        reRightCode = [] # 需要復權的股票
        # 得到数据库中最新一天的收盘价
        closeList = []
        for code in stockCodeList:
            sql = "select close from stockData_day where stockCode = %s order by date desc limit 1" 
            self.cur.execute(sql,code)
            result = self.cur.fetchone()
            if not result:
                closeList.append(0.0)
            else:
                closeList.append(result[0])
        # 查找今天的數據
        fields = ['pre_close']
        today = datetime.date.today().strftime('%Y%m%d') #今天的日期
        option = 'showblank=0.0;PriceAdj=F' # 要進行前復權處理
        wsd_data = w.wsd(stockCodeList,fields,today,today,option)
        if wsd_data.ErrorCode==0: # 返回成功
            stockData = wsd_data.Data[0]
            
        ifEqualList = (np.array(stockData) == np.array(closeList))
        for i in range(len(ifEqualList)):
            if not ifEqualList[i]:
                rate = (stockData[i] - closeList[i]) / closeList[i]
                reRightCode.append([stockCodeList[i],rate])
                
        print '需要复权的数量为'
        print len(reRightCode)
        print '需要复权的股票代码为：'
        print reRightCode
        logging.info('需要复权的股票代码为：%s' % reRightCode)
        return reRightCode
    
    def reRightData(self,reRightCode):
        '''复权数据'''
        print '开始复权数据'
        code = [record[0] for record in reRightCode]
        rate = [record[1] for record in reRightCode]
        
        paraList = []
        for i in range(len(reRightCode)):
            paraList.append((rate[i],)*6 + (code[i],))
             
        sql = 'update stockData_day set '+\
        'pre_close=pre_close*(1+%s),open=open*(1+%s),high=high*(1+%s),low=low*(1+%s),close=close*(1+%s),amt=amt*(1+%s)' +\
         " where stockCode=%s" 
        self.cur.executemany(sql,paraList)
        self.con.commit()
        print '日线复权完毕'
        
        paraList = []
        for i in range(len(reRightCode)):
            paraList.append((rate[i],)*5 + (code[i],))
            
        for period in [5,15,30,60]:
            sql = 'update stockData_%smin ' % period + \
            'set open=open*(1+%s),high=high*(1+%s),low=low*(1+%s),close=close*(1+%s),amt=amt*(1+%s)' + \
            " where stockCode=%s" 
            self.cur.executemany(sql,paraList)
            self.con.commit()
        print '分钟线复权完毕'
        
    
    def insertTodayData(self,stockCodeList):
        '''插入当天的数据'''
        fields = {'d':['pre_close','open','high','low','close','volume','amt','pct_chg'],
                  'm':['open','high','low','close','volume','amt','pct_chg']}
        startDate = {'d':datetime.date.today().strftime('%Y%m%d'),'m':datetime.date.today().strftime('%Y-%m-%d')+' 09:30:00'}
        endDate   = {'d':datetime.date.today().strftime('%Y%m%d'),'m':datetime.date.today().strftime('%Y-%m-%d')+' 15:00:00'}
        # day
        self.getDayData(stockCodeList,fields['d'],startDate['d'],endDate['d'],option='PriceAdj=F')
        # min
        for period in [5,15,30,60]:
            self.getMinData(stockCodeList,fields['m'],startDate['m'],endDate['m'],period)
        
        
    def getStockCode(self):
        '''获得股票代码'''
        sql = 'select distinct stockCode from stockCode'
        self.cur.execute(sql)
        stockCodeList = []
        for code in self.cur.fetchall():
            stockCodeList.append(code[0])
        return stockCodeList
    
    
    def close(self):
        '''關閉數據庫'''
        self.con.close()
        self.cur.close()
        
if __name__ == '__main__':
    
    stockData = StockData()
    
