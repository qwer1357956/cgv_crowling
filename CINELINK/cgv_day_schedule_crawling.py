from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

import pandas as pd
import cx_Oracle
import os
import csv
import json

theaterinfo, theatercode = {}, []

path = "./data/theater_data_cgv_megabox.json"
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
    f.close()

for i in data:
    if i['brand'] == 'CGV' and i['region'] == '서울':
        theatercode.append('{:0>4,}'.format(i['code']))

path = "./data/seoul_cgv.json"

with open(path, 'w', encoding='utf-8') as f:
    json.dump(theatercode, f)

su, areacd, resultall = 1, "01", []
date = datetime.now().strftime('%Y%m%d%H%M')[2:]
pathcsv = f"./data2/schedule_cgv_{date}.csv"

with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
    f.close()

for i in data:
    todaydate = str(date)[:-4]
    code = f"http://www.cgv.co.kr/reserve/show-times/?areacode={areacd}&theaterCode={i}&date={date}"
    driver = webdriver.Chrome()
    driver.get(code)
    driver.switch_to.frame("ifrm_movie_time_table")
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    link = soup.find_all("div", "info-movie")
    movielink = ''

    for lo in soup.find_all('a'):
        result, ticketlink = {}, ' '

        starttime = lo.get('data-playstarttime')
        endtime = lo.get('data-playendtime')
        detaillink = lo.get('href')
        strlo = str(lo)

        if '<a href="/movies/detail-view/?midx=' in strlo:
            titlestart = strlo.find('<strong>')
            titleend = strlo.find('</strong>')
            movietitle = strlo[titlestart+10:titleend].replace(' ','')

        if '/movies/detail-view/?midx=' in detaillink: movielink = lo.get('href')
        if '/ticket/?MOVIE_CD=' in detaillink: ticketlink = lo.get('href')

        if ticketlink == ' ': continue

        hour = int(endtime[:2]) - int(starttime[:2])
        min = int(endtime[2:]) - int(starttime[2:])
        tot = min + hour * 60 - 10

        result['brand'] = 'CGV'
        result['theatername'] = lo.get('data-theatername')
        result['theaternum'] = lo.get('data-screenkorname')
        result['movietitle'] = movietitle
        result['movieruntime'] = str(tot) + '분'
        result['moviedate'] = detaillink[60:68]
        result['moviestarttime'] = (str(starttime)[:2]+':'+str(starttime)[2:])[:5]
        result['theaterseatcnt'] = lo.get('data-seatremaincnt')+'석'
        resultall.append(result)

n_table = pd.DataFrame(resultall, columns=('brand', 'theatername', 'theaternum', 'movietitle', 'movieruntime', 'moviedate', 'moviestarttime', 'theaterseatcnt'))
n_table.to_csv(pathcsv, encoding='cp949', mode='w', index=False)

f = open(pathcsv, 'r', encoding='cp949')
data = csv.reader(f)

os.putenv('NLS_LANG', '.UTF8')
connection = cx_Oracle.connect('system', '1234', cx_Oracle.makedsn('127.0.0.1', 1521, 'XE'))
cursor = connection.cursor()

for i in data:
    if i[0] == 'brand': continue
    else:
        # 시간의 문제로 raw_movie_schedule에서 movie_schdule로 merge into 작업은 생략
        # sql = "INSERT INTO RAW_MOVIE_SCHEDULE(BRAND, THEATERNAME, THEATERNUM, MOVIETITLE, MOVIERUNTIME, MOVIEDATE, MOVIESTARTTIME, THEATERSEATCNT) values (:1,:2,:3,:4,:5,:6,:7,:8)"
        sql = "INSERT INTO MOVIE_SCHEDULE(BRAND, THEATERNAME, THEATERNUM, MOVIETITLE, MOVIERUNTIME, MOVIEDATE, MOVIESTARTTIME, THEATERSEATCNT) values (:1,:2,:3,:4,:5,:6,:7,:8)"
        val = (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7])
        cursor.execute(sql, val)
        connection.commit()

print('cgv 데이터 추가 성공')
