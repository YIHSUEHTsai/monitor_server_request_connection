#!/usr/bin/python3
import os
import sys
import csv
import pandas as pd
import subprocess
import time
import datetime
import requests
import paramiko 

prtg_username = "username=opapi"
prtg_pw = "passhash=4025917408"
prtg_url_us="https://prtg-slave.tutk.com/api/historicdata.csv?id="  #us
prtg_master_mlb_id = [12772,11315,11276,13041,12769,11282,11169,12600,12770,13173]
master_mlb_threshold = [250000,250000,950000,800000,120000,25000,950000,950000,950000,150000]

file_path = "/home/ubuntu/op_script/monitor_master_mlb_request/"

def download_prtg_file():
    time = datetime.datetime.now()
    year = time.year
    month = time.month
    day = time.day
    hour = "%02d"%int(time.hour)

    prtg_start_time = str(datetime.date(int(year), int(month), int(day))) + "-" + hour +"-00"
    prtg_end_time = str(datetime.date(int(year), int(month), int(day))) + "-" + hour +"-59"

    print (prtg_start_time)
    print (prtg_end_time)

    for index in range(len(prtg_master_mlb_id)):
        get_prtg_mlb_file_url = prtg_url_us + str(prtg_master_mlb_id[index]) + "&avg=0&sdate=" + prtg_start_time + "&edate=" + prtg_end_time + "&" + str(prtg_username) + "&" + str(prtg_pw)

        csv_file = requests.get(get_prtg_mlb_file_url)

        file_name = "master"+str(index+1)+"_mlb_raw.csv"
        with open(file_path+file_name, 'wb') as f:
            f.write(csv_file.content)

def parser_csv():
    for index in range(len(prtg_master_mlb_id)):
        file_name = "master"+str(index+1)+"_mlb_raw.csv"
        csv_data = pd.read_csv(file_path+file_name)

        mlb_value = csv_data['0x0001 Request Number(RAW)'].mean()

        if (mlb_value > master_mlb_threshold[index]):
            print ("service iotc restart!")
            master_domain  = "m"+str(index+1)+".iotcplatform.com"
            print (master_domain," ", mlb_value)
            time.sleep(30)
            iotc_restart(master_domain)

def iotc_restart(master_domain):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(hostname=master_domain, port=22, username='root', password='iotc@29045478')
    stdin, stdout, stderr = ssh.exec_command('sudo service iotc restart',timeout=60)
    #stdin, stdout, stderr = ssh.exec_command('ls',timeout=60)

    result = stdout.read()
    print (result)

    ssh.close()

if __name__ == "__main__":
    download_prtg_file()
    parser_csv()
