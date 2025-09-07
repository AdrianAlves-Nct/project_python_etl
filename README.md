# Projeto de Análise - E-Commerce (ETL + Pandas)

Este projeto tem como objetivo aplicar **ETL** (Extract, Transform, Load) e análises exploratórias usando Python e Pandas.

---

## 📂 Estrutura do Projeto
project_ecommerce/
│
├── data/
│ ├── raw/ # dados brutos (originais)
│ ├── processed/ # dados tratados/limpos
│
├── src/
│ ├── etl/ # funções de ETL
│ └── analysis/ # scripts de análise
│
├── output/    # tabelas e gráficos gerados pelas análises
├── reports/   # relatórios finais (pdf, docx, md, etc.)
│
├── etl_process.py # script principal para rodar o ETL
├── requirements.txt # dependências do projeto
└── README.md # documentação do projeto

🧹 Funcionalidades de ETL

Verificação e remoção de valores nulos

Conversão de colunas de data

Remoção de duplicados

Padronização de textos

📊 Próximas análises

Receita total por mês

Produto mais vendido e menos vendido

Ranking de vendedores

Cidades com maior volume de vendas

Ticket médio por compra

📌 Observações

Os dados brutos não são versionados (ignorados no .gitignore).

O foco do projeto é Pandas e análise de dados em Python.