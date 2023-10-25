import pandas as pd
import csv
import requests
from datetime import datetime

#CSV 파일 경로
file_path = "./data/theater_megabox.csv"
final_list = []
data_list = []

date = datetime.now().strftime('%Y%m%d%H%M')
today_date = int(date[:-4]) 


# CSV 파일 읽기
with open(file_path, newline='', encoding='utf-8') as csvfile:
    csv_reader = csv.reader(csvfile)
    for row in csv_reader:
        data_list.append(row)

result_list = []

for row in data_list :
    brchNo = row[0]

    url = 'https://www.megabox.co.kr/on/oh/ohc/Brch/schedulePage.do'

    parameters = {
    "masterType": "brch",
    "detailType": "area",
    "brchNo": brchNo,
    "firstAt": "N",
    "brchNo1": brchNo,
    "crtDe" : today_date,
    "playDe" : today_date
    }

    response = requests.post(url, data= parameters).json()['megaMap']['movieFormList']

    for i in response:
        theater_dict = {
                    "Brand" : "megabox",
                    "theaterName" : i['brchNm'],
                    "theaterNum" : i['theabExpoNm'],
                    "movieTitle" : i['movieNm'],
                    "movieRunTime" : i['moviePlayTime']+'분',
                    "movieDate": i['playDe'],
                    "movieStartTime" : i['playStartTime'],
                    "theaterSeatCnt" : str(i['restSeatCnt'])+'석',
                }
        final_list.append(theater_dict)
        df = pd.DataFrame(final_list)

# CSV 파일 경로
csv_file_path = f'./data2/schedule_megabox_{date}.csv'

df.to_csv(csv_file_path, encoding='cp949',index=False)

import cx_Oracle
import os

os.putenv('NLS_LANG', '.UTF8')
connection = cx_Oracle.connect('system', '1234', cx_Oracle.makedsn('127.0.0.1', 1521, 'XE'))

cursor = connection.cursor()

# CSV 파일을 DataFrame으로 읽어오기
df = pd.read_csv(csv_file_path, encoding='cp949')

# DataFrame을 반복하여 데이터를 Oracle DB에 삽입
for index, row in df.iterrows():
    theaterName = row['theaterName']
    theaterNum = row['theaterNum']
    movieTitle = row['movieTitle']
    movieRunTime = row['movieRunTime']
    movieDate = row['movieDate']
    movieStartTime = row['movieStartTime']
    theaterSeatCnt = row['theaterSeatCnt']

    # INSERT 쿼리 실행
    sql_query = """
    INSERT INTO MOVIE_SCHEDULE (theaterName, theaterNum, movieTitle, movieRunTime, movieDate, movieStartTime, theaterSeatCnt)
    VALUES (:1, :2, :3, :4, :5, :6, :7)
    """
    val = (theaterName, theaterNum, movieTitle, movieRunTime, movieDate, movieStartTime, theaterSeatCnt)
    cursor.execute(sql_query, val)
    connection.commit()

# 커서와 연결 닫기
cursor.close()
connection.close()
