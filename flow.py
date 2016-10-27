# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 14:47:38 2016

@author: yiran.zhou
"""

import pandas as pd
import numpy as np
import os


# 累加函数
def accum(s):
    i = 1
    while i < len(s):
        s[i] = s[i-1] + s[i]
        i += 1
    return s


# 根据每日flow计算累计flow
def accumFlow(data):
    #先转换成每列一个ticker，计算累计flow
    d = data.copy()
    tickerList = d.ix[:, 'Ticker'].unique().tolist()
    l = []
    for a in tickerList:
        s = d[d['Ticker'] == a].ix[:,'Flow'].copy()
        s = accum(s)
        s.name = a
        l.append(s)
    dataAccum = pd.DataFrame()
    i = 0
    while i < len(l):
        dataAccum = pd.concat([dataAccum, l[i]], axis = 1)
        i += 1
    dataAccum = dataAccum.sort_index()   
    dataAccum.index.name = 'Date'
    dataAccum = dataAccum.fillna(method = 'ffill').fillna(0)
    #再转换成行格式，每行一个ticker一个flow，匹配彭博的BBU功能。
    res = pd.DataFrame(columns = ['Ticker', 'Flow'])
    i = 0
    while i < len(dataAccum.index):
        j = 0
        while j < len(dataAccum.columns):
            tmp = pd.DataFrame({'Ticker':dataAccum.columns[j], 'Flow':dataAccum.ix[i, j]}, index = [dataAccum.index[i]])
            res = pd.concat([res, tmp], axis = 0)
            j += 1
        i += 1
    return res



# 读取EPFR数据，把每个类别转换成一列（暂时没用）
def readEPFR(path):
    # 读取EPFR数据,虽然后缀是xlsx，但其实是html
    dataRaw = pd.read_html(path)[0]
    #dataRaw.columns = dataRaw.ix[0, :].tolist()
    dataRaw.columns = ['Date', 'Asset', 'Flow']
    dataRaw = dataRaw[1:]
    i = 1    
    # 读完转换格式
    while i <= len(dataRaw.index):
        dataRaw.ix[i, 0] = pd.Timestamp(dataRaw.ix[i, 0])
        dataRaw.ix[i, 2] = float(dataRaw.ix[i, 2])
        i += 1
    dataRaw.set_index(dataRaw.columns[0], inplace = True)    
    # 把每个asset做成一列
    assetList = dataRaw.ix[:, 0].unique().tolist()
    l = []
    for a in assetList:
        s = dataRaw[dataRaw['Asset'] == a].ix[:,'Flow']
        s.name = a
        l.append(s)
    data = pd.DataFrame()
    i = 0
    while i < len(l):
        data = pd.concat([data, l[i]], axis = 1)
        i += 1
    data = data.sort_index()   
    data.index.name = 'Date'
    return data
    
        
EPFR2TIC_1 = {
    'All Emerging Markets-FF-Bond':'.EMBF Index',
    'All Emerging Markets-FF-Equity':'.EMEF Index',
    'Germany-Western Europe-Long Term Government-Bond':'.GELTGBF Index',
    'Japan-Asia Pacific-Equity':'.JPEF Index',
    'Japan-Asia Pacific-Long Term Government-Bond':'.JPLTGBF Index',
    'United Kingdom-Western Europe-Long Term Government-Bond':'.UKLTGBF Index',
    'USA-All MM Funds-MM':'.USMMF Index',
    'USA-North America-Equity':'.USEF Index',
    'USA-North America-Long Term Government-Bond':'.USLTGBF Index',
    'Western Europe-All MM Funds-MM':'.EUMMF Index',
    'Western Europe-FF-Equity':'.EUEF Index'
}        
        
# 读取EPFR数据，加一列彭博ticker
def readEPFR2(path, dic):
    # 读取EPFR数据,虽然后缀是xlsx，但其实是html
    dataRaw = pd.read_html(path)[0]
    #dataRaw.columns = dataRaw.ix[0, :].tolist()
    dataRaw.columns = ['Date', 'Asset', 'Flow']
    dataRaw = dataRaw[1:]
    i = 1    
    # 读完转换格式
    while i <= len(dataRaw.index):
        dataRaw.ix[i, 0] = pd.Timestamp(dataRaw.ix[i, 0])
        dataRaw.ix[i, 2] = float(dataRaw.ix[i, 2])
        i += 1
    dataRaw.set_index(dataRaw.columns[0], inplace = True)    
    # 用一组映射，加入彭博的自定义ticker
    dataRaw['Ticker'] = dataRaw['Asset'].map(dic)
    dataRaw.index = dataRaw.index.strftime('%Y/%m/%d')
    return dataRaw
    
# 主程序    
def proc(suffix, dic):
    #先看是不是第一次运行，如果以前运行过直接读取结果
    histDataPath = 'EPFROutput' + '_' + suffix + '.xls'
    dataPath = 'EPFR' + '_' + suffix + '.xlsx'
    dataPath2 = 'EPFRAccum' + '_' + suffix + '.xlsx'
    if os.path.isfile(dataPath) == True:
        data = pd.read_excel(dataPath)
    else: #以前没有运行过，读取EPFR的历史数据
        data = readEPFR2(histDataPath, dic)   
        #data.to_excel('EPFR.xls')    
    
    #读取增量数据
    subIncFdr = 'INCREMENTAL' + '_' + suffix
    incPath = os.getcwd() + '\\' + subIncFdr
    fileList = os.listdir(incPath)
    for f in fileList:
        path = incPath + '\\' + f
        incData = readEPFR2(path, dic)
        data = pd.concat([data, incData])
        data = data.drop_duplicates()
        data = data.sort_index()
    data.to_excel(dataPath)
    dataAccum = accumFlow(data)
    dataAccum.to_excel(dataPath2)
    return data, dataAccum

# 读入EPFR数据
# 一组数据包含3个部分，第一次读入的历史数据‘EPFROutput_1.xls’, 增量数据文件夹‘INCREMENTAL_1’, 导入彭博的结果文件‘EPFR_1.xlsx’
# 可以有多组，区分在于后缀。要新加一组就可以是‘EPFROutput_2.xls’,'INCREMENTAL_2','EPFR_2.xlsx',然后建一个dict映射
if __name__ == '__main__':
    suffix = '1'
    data, dataAccum = proc(suffix, EPFR2TIC_1)
    