from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium import webdriver

import pandas as pd
import cx_Oracle
import os
import csv

code = "http://www.cgv.co.kr/ticket/"

driver = webdriver.Chrome()
driver.get(code)

driver.switch_to.frame("ticket_iframe")

r = driver.page_source
soup = BeautifulSoup(r, "html.parser")

link = soup.find_all("div", "movie-list.nano.has-scrollbar-y")

linklist, linklast, linkL = [], [], []

for lo in soup.find_all('li'):
    if lo != None: linklist.append(lo.get('movie_idx'))
    elif lo == None: pass

for i in linklist:
    if i == None: pass
    else: linklast.append(i)

url = "http://www.cgv.co.kr/movies/detail-view/?midx="

# url 생성
for i in linklast:
    if i == '': 
        continue
    else: 
        linkL.append(url+i)
    
resultall, infoall, num = [], [], 0

for i in linkL:
    driver = webdriver.Chrome()
    driver.get(i)

    try:
        # 경고창 제거
        alert = driver.switch_to.alert
        alert.accept()

    except:
        # 경고 창이 뜨지 않을 경우 정상 진행
        r = driver.page_source
        soup = BeautifulSoup(r, "html.parser")

        menu = driver.find_elements(by=By.CLASS_NAME, value='sect-base-movie')

        su, infotitlelist, infolist = 1, [], []

        for i in menu:
            movieinfo = {}
            
            # 영화 제목 데이터 가공
            title = i.find_element(By.CLASS_NAME, value='title').text.split('\n')[0]
            title = title[:title.find('현재상영중')]
            title = title[:title.find('예매중')]

            info = i.find_element(By.CLASS_NAME, value='spec').text.split('\n')
            code = linkL[num][-5:]

            for j in info:
                infoend = j.find('공식사이트')
                if '공식사이트' in j: j = j[:infoend]

                j = j.replace(' /', '')
                j = j.replace(' ', '')
                j = j.replace(':', '')

                if 'www.' or '.com' or 'co.kr' or 'kr/' in j:
                    pass
                else:
                    if su % 2 == 1:
                        infotitlelist.append(j)
                    elif su % 2 == 0:
                        infolist.append(j)
                    su += 1

            info_m = infotitlelist[-2].split(',')
            movieinfo['TITLE'] = title

            if len(infolist) < 4:
                movieinfo['DIRECTOR'] = ''
                movieinfo['ACTOR'] = ''
                movieinfo['GENRE'] = ''

            else:
                movieinfo['DIRECTOR'] = infolist[0]
                movieinfo['ACTOR'] = infolist[1]
                movieinfo['GENRE'] = infotitlelist[2][2:]

            if len(info_m) < 3:
                movieinfo['SPECTATORS'] = ' '
                movieinfo['RUNTIME'] = ' '
            else:
                movieinfo['SPECTATORS'] = info_m[0]
                movieinfo['RUNTIME'] = info_m[1]

            movieinfo['OPENDATE'] = infotitlelist[-1]
            movieinfo['IMGLINK'] = f'https://img.cgv.co.kr/Movie/Thumbnail/Poster/000087/{code}/{code}_1000.jpg'

            menu2 = driver.find_element(by=By.CLASS_NAME, value='sect-story-movie').text
            movieinfo['MOVIECONTENT'] = menu2.replace('\n', ' ')

        if len(movieinfo) != 0: infoall.append(movieinfo)
        num += 1

date = datetime.now().strftime('%Y%m%d%H%M')[2:]
pathcsv = f"./data2/movie_detail_{date}.csv"

n_table = pd.DataFrame(infoall, columns=('TITLE', 'DIRECTOR', 'ACTOR', 'GENRE', 'SPECTATORS', 'RUNTIME', 'OPENDATE', 'IMGLINK', 'MOVIECONTENT'))
n_table.to_csv(pathcsv, encoding='cp949', mode='w', index=False)

f = open(pathcsv, 'r', encoding='cp949')
data = csv.reader(f)

os.putenv('NLS_LANG', '.UTF8')
connection = cx_Oracle.connect('system', '1234', cx_Oracle.makedsn('127.0.0.1', 1521, 'XE'))
cursor = connection.cursor()

for i in data:
    if i[0] == 'title': continue
    else:
        # sql = "INSERT INTO RAW_MOVIE(TITLE, DIRECTOR, ACTOR, GENRE, SPECTATORS, RUNTIME, OPENDATE, IMGLINK, MOVIECONTENT) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)"
        sql = "INSERT INTO MOVIE(TITLE, DIRECTOR, ACTOR, GENRE, SPECTATORS, RUNTIME, OPENDATE, IMGLINK, MOVIECONTENT) values (:1,:2,:3,:4,:5,:6,:7,:8,:9)"
        val = (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8])
        cursor.execute(sql, val)
        connection.commit()

print('영화 상세 데이터 추가 성공')