import pandas as pd
def export_csv_to_parquet():
    df = pd.read_csv('./data/cars.csv')
    df.to_parquet('./temp/output.parquet')