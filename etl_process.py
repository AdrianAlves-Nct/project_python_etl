from src.etl import etl_utils
import pandas as pd
import os

#pegando o arquivo bruto e transformando em Dataframe
df_sallers = pd.read_csv('data/raw/sales_augmented.csv', sep=',')

print(df_sallers.head())
print(df_sallers.dtypes)

#limpeza do Dataframe com pipeline
df_sallers_limpo = etl_utils.pipeline_etl(df=df_sallers, colunas_texto=['customer_city', 'product_name'], colunas_data=['order_date'], valores_corrigir=True, remover_nulos_flag=True)

#ordenando os valores ver se os dados recentes estão no começo ou no fim.
df_sallers_limpo = df_sallers_limpo.sort_values(by='order_id')

#removendo os ids duplicados.
df_sallers_limpo = etl_utils.pipeline_etl(df=df_sallers_limpo, colunas_dup=['order_id'], ascending_dup=False)
print(df_sallers_limpo.head())
print(df_sallers.dtypes)

#salvando o arquivo atualizado, limpo e processado em csv para futuras análises.
os.makedirs('data/processed', exist_ok=True)
df_sallers_limpo.to_csv('data/processed/sellers_processed.csv', index=False)