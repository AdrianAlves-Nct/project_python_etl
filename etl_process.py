from src.etl import etl_utils
import pandas as pd

df_sallers = pd.read_csv('data/raw/sales_augmented.csv', sep=',')

print(df_sallers.head())
print(df_sallers.dtypes)
df_sallers_limpo = etl_utils.pipeline_etl(df=df_sallers, colunas_data=['order_date'])
print(df_sallers.dtypes)