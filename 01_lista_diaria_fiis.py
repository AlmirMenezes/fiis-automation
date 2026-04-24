import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime

BASE_URL = "https://investidor10.com.br/fiis/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 🔁 sessão reutilizável (melhor performance)
session = requests.Session()
session.headers.update(HEADERS)


def tratar_numero(valor):
    if not valor:
        return None

    valor = valor.strip()

    if valor in ["-", ""]:
        return None

    try:
        return float(valor.replace("%", "").replace(".", "").replace(",", "."))
    except:
        return None


def request_com_retry(url, tentativas=3):
    for tentativa in range(tentativas):
        try:
            response = session.get(url, timeout=30)

            if response.status_code == 200:
                return response

            print(f"Status {response.status_code} - tentativa {tentativa+1}")

        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e} - tentativa {tentativa+1}")

        time.sleep(2)

    return None


def scrape_pagina(page):
    url = f"{BASE_URL}?page={page}"
    response = request_com_retry(url)

    if not response:
        print(f"Falha ao acessar página {page}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    tabela = soup.find("table")
    if not tabela:
        return []

    tbody = tabela.find("tbody")
    if not tbody:
        return []

    linhas = tbody.find_all("tr")
    dados = []

    for linha in linhas:
        colunas = linha.find_all("td")

        # 🔒 segurança contra quebra de layout
        if len(colunas) < 8:
            continue

        try:
            piece = colunas[0].text.strip().split("\n")

            if len(piece) < 8:
                continue

            dados.append({
                "data_coleta": datetime.now().strftime("%Y-%m-%d"),
                "ticker": piece[6].strip(),
                "descricao": piece[7].strip(),
                "patrimonio": colunas[1].text.strip(),
                "p_vp": tratar_numero(colunas[2].text),
                "dy": tratar_numero(colunas[3].text),
                "dy_5anos": tratar_numero(colunas[4].text),
                "liquidez": colunas[5].text.strip(),
                "tipo": colunas[6].text.strip(),
                "segmento": colunas[7].text.strip(),
            })

        except Exception as e:
            print(f"Erro ao processar linha: {e}")
            continue

    return dados


def scrape_todas_paginas(max_paginas=20):
    todos_dados = []

    for page in range(1, max_paginas + 1):
        print(f"📄 Scraping página {page}...")

        dados = scrape_pagina(page)

        # 🔴 parada inteligente
        if not dados:
            print(f"Fim detectado na página {page}")
            break

        todos_dados.extend(dados)

        time.sleep(1)  # evita bloqueio

    return pd.DataFrame(todos_dados)


# 🚀 execução
df_fiis = scrape_todas_paginas()

print(f"\nTotal de FIIs coletados: {len(df_fiis)}")

# 💾 salvar arquivo
df_fiis.to_csv(
    "fiis_lista.csv",
    index=False,
    encoding="utf-8-sig",
    sep=";",
    decimal=","
)
