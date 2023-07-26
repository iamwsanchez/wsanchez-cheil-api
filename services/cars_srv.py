import pandas as pd
import csv

def export_csv_to_parquet():
    df = pd.read_csv('./data/cars.csv')
    df.to_parquet('./temp/output.parquet')
    
def read_csv():
    with open('./data/cars.csv') as f:
        reader = csv.reader(f)
        car_list = list(reader)
    return car_list

def data_from_csv():
    with open("./data/cars.csv", "r") as file:
        reader = csv.DictReader(file, delimiter=",")
        data = list(reader)
    return data    