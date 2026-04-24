# %%
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

from sqlalchemy import create_engine

driver = 'ODBC Driver 17 for SQL Server'
server = 'DESKTOP-VCNMA0D\\SQLEXPRESS'  # note o escape da barra invertida
database = 'DB_INVESTE'

# String de conexão com autenticação integrada
conn_str = (
    f"mssql+pyodbc://@{server}/{database}"
    f"?driver={driver.replace(' ', '+')}"
    f"&trusted_connection=yes"
)

engine = create_engine(conn_str)
connection = engine.connect()


# %%
def tratar_numero(valor):
    valor = valor.strip()

    if valor in ["-", "", None]:
        return None  # ou 0, dependendo da sua regra

    return float(valor.replace("%", "").replace(".", "").replace(",", "."))

# %%


BASE_URL = "https://investidor10.com.br/fiis/"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def scrape_pagina(page):
    url = f"{BASE_URL}?page={page}"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    tabela = soup.find("table")
    dados = []

    if not tabela:
        return dados

    tbody = tabela.find("tbody")
    if not tbody:
        return dados

    linhas = tbody.find_all("tr")

    for linha in linhas:
        colunas = linha.find_all("td")

        #dividir apenas a primeira coluna, pois as outras podem conter quebras de linha
        piece = colunas[0].text.strip().split("\n")
        # print(colunas)

        if len(colunas) > 0:
            dados.append({
                "data_coleta": time.strftime("%d-%m-%Y"),
                "ticker": piece[6],
                "descricao": piece[7],
                "patrimonio": colunas[1].text.strip(),
                "p_vp": tratar_numero(colunas[2].text.strip()),
                "dy": tratar_numero(colunas[3].text.strip()),
                "dy_5anos": tratar_numero(colunas[4].text.strip()),
                "liquidez": (colunas[5].text.strip()),
                "tipo": colunas[6].text.strip(),
                "segmento": colunas[7].text.strip(),
                # "variacao_12m": tratar_numero(colunas[8].text.strip()),
                # "variacao_24m": tratar_numero(colunas[9].text.strip()),
                # "variacao_60m": tratar_numero(colunas[10].text.strip()),
            })

    return dados


def scrape_todas_paginas():
    page = 1
    todos_dados = []

    while True:
        print(f"Scraping página {page}...")

        dados = scrape_pagina(page)

        # 🔴 condição de parada
        if not dados:
            print(f"Nenhum dado encontrado na página {page}. Fim do scraping.")
            break

        todos_dados.extend(dados)

        page += 1
        time.sleep(1)

    return pd.DataFrame(todos_dados)


# execução
df_fiis = scrape_todas_paginas()

# print(df_fiis.head())
print(f"\nTotal de FIIs coletados: {len(df_fiis)}")

# %%
df_fiis.head()

# %%
df_fiis.to_csv("fiis_lista.csv", index=False, encoding="utf-8-sig", sep=";",decimal=",")

# %%
df_fiis.to_sql('tb_lista_fiis',connection, if_exists='append', index=False)


