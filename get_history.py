import csv
import json
import time
import requests
from datetime import datetime

class Option:
    def __init__(self, symbol, interval, start_time, end_time):
        self.symbol = symbol
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

def construct_query_string(params, prefix=''):
    keys = sorted(params.keys())
    qs = ''
    for i, k in enumerate(keys):
        v = params[k]
        if isinstance(v, dict):
            qs += construct_query_string(v, k)
        elif isinstance(v, list):
            nested_map = {str(i): v[i] for i in range(len(v))}
            qs += construct_query_string(nested_map, k)
        else:
            if prefix:
                qs += f"{prefix}[{k}]={v}"
            else:
                qs += f"{k}={v}"
        if i != len(keys) - 1:
            qs += "&"
    return qs

def write_file(data_objs, skip_header):
    with open('klines.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if not skip_header:
            header = ["Open time", "Open", "High", "Low", "Close", "Volume", 
                      "Close time", "Quote asset volume", "Number of trades", 
                      "Taker buy base asset volume", "Taker buy quote asset volume"]
            writer.writerow(header)
        for data in data_objs:
            row = [
                str(data[0]), data[1], data[2], data[3], data[4],
                data[5], str(data[6]), data[7], str(data[8]), data[9], data[10]
            ]
            writer.writerow(row)

def main():
    now = int(time.time() * 1000)
    duration = 3600 * 1000
    now = 1715747119000
    start_time = now - duration
    end_time = now
    skip_header = False
    
    while True:
        options = Option("btcthb", "1m", start_time, end_time)
        params = options.__dict__
        qs = construct_query_string(params)
        url = "https://api.binance.th/api/v1/klines"
        
        millis = start_time
        seconds = millis // 1000
        nanoseconds = (millis % 1000) * 1000000
        timestamp = datetime.fromtimestamp(seconds + nanoseconds / 1e9)
        formatted_time = timestamp.strftime("%d/%m/%Y %H:%M:%S")
        print(f"\nstart time {formatted_time} call {url}?{qs}")

        response = requests.get(f"{url}?{qs}")
        if response.status_code != 200:
            print(response)
            break
        print(response.status_code)
        
        data_obj = json.loads(response.content)
        write_file(data_obj, skip_header)
        skip_header = True
        end_time = start_time
        start_time = end_time - duration

        time.sleep(2)

if __name__ == "__main__":
    main()
