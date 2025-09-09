import pandas as pd
import numpy as np

def verificar_nulos(df):
    """
    Esta função verifica os registro nulos de uma tabela.

    Args:
        O dataframe que quer verificar.
       
    Returns:
        Retorna as colunas do dataframe com a quantidade de nulos em cada uma delas.
    """
    return df.isna().sum()


def remover_nulos(df):
    """
    Esta função complementa a 'verificar nulos', removendo as linhas nulas que encontrar (minímo de um valor).

    Args: 
        O dataframe que quer verificar.

    Returns: 
        Retorna o dataframe sem valores nulo.
    """
    return df.dropna(how='any')


def verificar_data(df, coluna):
    """
    Esta função verificar se a coluna de data está no formato correto.
    Se não tenta transformar a coluna em data, e se achar um erro, transforma em NaT.

    Args:
        argumento1: O seu dataframe.
        argumento2: A coluna que deseja verificar.
    Returns:
        Retorna a coluna do dataframe formatada em datetime e os erros em NaT.
    """
    try:
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
        return df[coluna]
    except Exception as ex:
        print(f'Ocorreu um erro: {ex}')
        df[coluna] = pd.NaT
        return df[coluna]


def verificar_duplicados(df, *coluna: str):
    """
    Esta função verificar se a coluna tem valores duplicados.

    Args:
        argumento1: O seu dataframe.
        argumento2: As colunas que deseja verificar.
    Returns:
        Retorna uma Series contendo valores booleanos se é duplicado ou não, True = Sim, False = Não.
    """
    return df.duplicated(subset=list(coluna), keep=False)
                         

def remover_duplicados(df, *coluna, ascending: bool = True):
    """
    Esta função remove os valores duplicados em seu dataframe.

     Args:
        argumento1: O seu dataframe.
        argumento2: A coluna que deseja verificar se tem o valor duplicado.
        argumento3: (Opcional) Vai ser a forma como achar os dados, se de cima para baixo ou de baixo para cima. Se os seus registros mais atualizados estão no fim, coloque False para ser pegos primeiro e armazenados. Se estão no início, deixe como está. 
    Returns:
        Retorna o dataframe sem valores duplicados de acordo com a coluna e o ascending passado.
    """
    df = df.sort_values(by=list(coluna), ascending = ascending)
    df.drop_duplicates(subset=list(coluna), inplace=True)
    return df


def padronizar_txt(df, *coluna):
    """
    Esta Função remove valores vazios e None para NaN e padroniza o texto da(s) sua(s) coluna(s) em maiúsculo.
    Args:
        argumento1: O seu dataframe.
        argumento2: A(s) coluna(s) que deseja padronizar.
    Returns:
        Retorna a(s) coluna(s) do dataframe formatada.
    """
    try:
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df.replace([None], np.nan, inplace=True)

        for i in coluna:
            if i in df.columns and df[i].dtype == 'object':
                df[i] = df[i].astype(str).str.upper().replace('NAN', np.nan)

        return df[list(coluna)]
    except Exception as ex:
        print(f'Ocorreu um erro {ex}')
        return df[list(coluna)]


def corrigir_valores(df):
    """
    Função para corrigir valores numéricos. Ela reconhece as colunas numéricas automáticamente e faz a correção:
        valor negativo, vira positivo
        valor zero, vira NaN, para um tratamento posterior.

    Args:
        argumento1: O seu dataframe.
    Returns:
        retorna seu dataframe com as colunas numéricas padronizadas.
    """
    try:
        num_cols = df.select_dtypes(include=['number']).columns
        for i in num_cols:
            df[i] = df[i].apply(
                lambda x: abs(x) if pd.notnull(x) and x < 0 else (np.nan if x == 0 else x)
            )
        return df
    except Exception as ex:
        print(f'Erro ao corrigir valores: {ex}')
        return df
    


def pipeline_etl(df, colunas_texto=None, colunas_data=None, colunas_dup=None, ascending_dup=True, remover_nulos_flag=True):
    """
    Pipeline para ETL de dataframes.

    Args:
        df: DataFrame de entrada.
        colunas_texto: Lista de colunas que serão padronizadas em maiúsculo.
        colunas_data: Lista de colunas que serão convertidas para datetime.
        colunas_dup: Lista de colunas para verificar e remover duplicados.
        ascending_dup: Define a ordem para remoção de duplicados (True = do início para o fim).
        remover_nulos: Booleano para remover linhas com qualquer valor nulo.
        
    Returns:
        DataFrame processado.
    """

    # Padronizar colunas de texto
    if colunas_texto:
        padronizar_txt(df, *colunas_texto)

    # Converter colunas para datetime
    if colunas_data:
        for col in colunas_data:
            verificar_data(df, col)

    # Corrigir valores numéricos (negativos e zeros)
    corrigir_valores(df)

    # Remover duplicados
    if colunas_dup:
        df = remover_duplicados(df, *colunas_dup, ascending=ascending_dup)

    # Remover nulos
    if remover_nulos_flag:
        df = remover_nulos(df)

    return df



data_test =  {
    "order_id": [1, 2, 2, 3, None, 5, 6, 6],
    "order_date": [
        "2021-01-05", 
        "2021-01-07", 
        "2021-01-07", 
        "2021-13-01",  # data inválida
        None, 
        "2021-02-15", 
        "2021-03-01", 
        "not_a_date"   # valor errado
    ],
    "customer_city": [
        "São Paulo", 
        "Rio de Janeiro", 
        None, 
        "Curitba",      # erro de digitação
        " ",            # string vazia
        "Belo Horizonte", 
        "São Paulo", 
        "São Paulo"
    ],
    "product_name": [
        "Notebook Gamer", 
        "Mouse Sem Fio", 
        "Mouse Sem Fio", 
        "Monitor 24''", 
        "Teclado Mecânico", 
        None, 
        "Headset Bluetooth", 
        "Monitor 24''"
    ],
    "price": [
        4500, 
        120, 
        -50,      # preço inválido
        900, 
        None, 
        300, 
        265, 
        900
    ],
    "quantity": [
        1, 
        2, 
        0,        # quantidade inválida
        1, 
        None, 
        1, 
        -3,       # quantidade inválida
        1
    ]
}


df = pd.DataFrame(data_test)


df_limpo = pipeline_etl(df, colunas_texto=['customer_city', 'product_name'], colunas_data=['order_date'], colunas_dup=['order_id'], ascending_dup=True, remover_nulos_flag=False)
print(df_limpo)