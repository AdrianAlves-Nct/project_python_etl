from src.etl import etl_utils
import pandas as pd

df_sallers = pd.read_csv('data/raw/sales.csv', sep=',')

print(df_sallers.head())