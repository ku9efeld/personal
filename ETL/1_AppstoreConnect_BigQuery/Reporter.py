#!/usr/bin/env python
# coding: utf-8
import subprocess
from subprocess import Popen, PIPE
import pandas as pd
import enum
import gzip
import shutil,os
import re
class Report():
    date = str()
    name = str()
    id = int()
    def __init__(self,name,date,id):
        self.name=name
        self.id=id
        self.date=date
    def __repr__(self):
        base_command = 'java -jar Reporter.jar p=Reporter.properties Sales.getReport'
        if(self.name == 'Subscriber'):
            return base_command + f' {self.id}, {self.name}, Detailed, Daily, {self.date}, 1_3'
        elif(self.name == 'SubscriptionEvent'):
            return base_command + f' {self.id}, {self.name}, Summary, Daily, {self.date}, 1_3'
        else:
            return 'Not exist report command'
class Reporter():
    names = ['SubscriptionEvent','Subscriber']
    dates = list()
    id = int()
    path_dst = str()
    # Init
    def __init__(self,start_date,end_date,name,id,path_dst): #name - scalar
        self.dates = pd.date_range(start=start_date,end=end_date,freq='1D').strftime('%Y%m%d').tolist()
        #
        if isinstance(name,str):
            flag = False
            if name == 'SubscriptionEvent':
                mask = 'Subscriber'
                flag = True
            elif name == 'SubscriptionEvent':
                mask = 'Subscriber'
                flag = True
        if flag: self.names = self.names.remove(mask)
        #  
        self.id = id
        self.path_dst = path_dst
    def result_filename(self,stdout):
        pattern = r"Successfully downloaded (.*?)\\r\\n"
        # Поиск совпадений
        match = re.search(pattern, stdout)
        # Если найдено совпадение, извлекаем часть строки
        return match.group(1) if match else 'Unsuccessful try'
    # Unzip
    def unzip_report(self,filename):
        with gzip.open(filename, 'rb') as file_in:
            file = os.path.join(self.path_dst,filename[:-3:])
            with open(file, 'wb') as file_out:
                shutil.copyfileobj(file_in, file_out)
        os.remove(filename)
        return None
    def update_dates(self,dates):
        self.dates=dates
    # Extract  
    def extract(self):
        try:
            for name in self.names:
                for date in self.dates:
                    p = subprocess.Popen(str(Report(name,date,self.id)),
                    shell=True,
                    stdout=PIPE
                    )
                    stdout, _ = p.communicate()
                    result = str(stdout)
                    filename = self.result_filename(result)
                    if filename == 'Unsuccessful try':
                        print(f'Не вышло скачать отчет {name} за {date}')
                    else:
                        self.unzip_report(filename)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with return code {e.returncode}")
    def show(self):
        return f'Типы отчетов {self.names}, c {self.dates[0]} по {self.dates[-1]}, в директорию - {self.path_dst}'