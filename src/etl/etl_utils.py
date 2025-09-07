import pandas as pd



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

    Args:
        argumento1: O seu dataframe.
        argumento2: A coluna que deseja verificar.
    Returns:
        Retorna a coluna do dataframe formatada em datetime.
    """
    if df[coluna].dtype != 'datetime64[ns]':
        df[coluna] = pd.to_datetime(df[coluna])
    return df[coluna]


def remover_duplicados(df, coluna):
    """
    Esta função remove os valores duplicados em seu dataframe.

     Args:
        argumento1: O seu dataframe.
        argumento2: A coluna que deseja verificar se tem o valor duplicado.
    Returns:
        Retorna o dataframe sem valores duplicados de acordo com a coluna passada.
    """
    return df.drop_duplicates(subset=[coluna])


def padronizar_txt(df, coluna):
    """
    Esta função padroniza o texto da sua coluna em minúsculo.
    Args:
        argumento1: O seu dataframe.
        argumento2: A coluna que deseja padronizar.
    Returns:
        Retorna a coluna do dataframe formatada minúsculo.
    """
    if df[coluna].dtype not in [int, float, 'datetime64[ns]']:
        return df[coluna].str.lower()
    else:
        print(f'A coluna {coluna}, é do tipo {df[coluna].dtypes} e por isso não poder ser padronizada.')



