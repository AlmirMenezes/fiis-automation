#!/usr/bin/env python
# coding: utf-8

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


# =========================================
# 🔧 CONFIGURAÇÕES
# =========================================

BASE_URL = "https://investidor10.com.br/fiis/"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Referer": "https://investidor10.com.br/"
}

INPUT_FILE = "fiis_lista.csv"
OUTPUT_FILE = "fiis_detalhes.csv"


# =========================================
# 🔢 TRATAR NÚMEROS
# =========================================

def tratar_numero(valor):
    if not valor:
        return None

    valor = valor.strip()

    if valor in ["-", "", None]:
        return None

    try:
        return float(
            valor
            .replace("%", "")
            .replace("R$", "")
            .replace(".", "")
            .replace(",", ".")
        )
    except:
        return None


# =========================================
# 🔁 SESSÃO COM RETRY
# =========================================

def criar_sessao():
    session = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    return session


# =========================================
# 📥 CARREGAR TICKERS
# =========================================

def carregar_tickers():
    df = pd.read_csv(INPUT_FILE, sep=";", decimal=",")
    df = df[["ticker"]].drop_duplicates()
    return df


# =========================================
# 🔎 SCRAPING DE UM FII
# =========================================

def extrair_dados_fii(session, ticker):
    url = f"{BASE_URL}{ticker}"

    try:
        response = session.get(url, headers=HEADERS, timeout=30)

        if response.status_code != 200:
            print(f"❌ {ticker} - erro {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # ===============================
        # 📊 CARD PRINCIPAL
        # ===============================
        cards = soup.select_one("section#cards-ticker")

        if not cards:
            print(f"⚠️ {ticker} - sem dados")
            return None

        # cotação
        valor_tag = cards.select_one("div._card.cotacao span.value")
        valor = valor_tag.text.strip() if valor_tag else None

        # pvp
        pvp_tag = cards.select_one("div._card.vp span")
        pvp = tratar_numero(pvp_tag.text) if pvp_tag else None

        # liquidez
        liquidez_tag = cards.select_one("div._card.val span")
        liquidez = liquidez_tag.text.strip() if liquidez_tag else None

        # ===============================
        # 🏢 INFO EMPRESA
        # ===============================
        segmento = None
        num_cotista = None
        cotas_emitidas = None
        ult_rend = None

        tabela = soup.select_one("div#table-indicators")

        if tabela:
            cells = tabela.find_all("div", class_="cell")

            if len(cells) > 11:
                segmento = cells[4].text.strip()
                num_cotista = tratar_numero(cells[10].text)
                cotas_emitidas = tratar_numero(cells[11].text)
                ult_rend = tratar_numero(cells[-1].text)

        # ===============================
        # 💰 DIVIDENDOS
        # ===============================
        yield_1_mes = None
        yield_3_mes = None

        divs = soup.find("section", {"id": "dividend-group"})

        if divs:
            valores = divs.find_all("span", class_="content--info--item--value")

            if len(valores) >= 3:
                yield_1_mes = tratar_numero(valores[0].text)
                yield_3_mes = tratar_numero(valores[2].text)

        # ===============================
        # 📦 RESULTADO
        # ===============================
        if valor and valor != "R$ -":
            return {
                "data_coleta": datetime.now().strftime("%Y-%m-%d"),
                "ticker": ticker,
                "cotacao": tratar_numero(valor),
                "pvp": pvp,
                "liquidez": liquidez,
                "num_cotista": num_cotista,
                "cotas_emitidas": cotas_emitidas,
                "ult_rend": ult_rend,
                "yield_1_mes": yield_1_mes,
                "yield_3_mes": yield_3_mes,
                "segmento": segmento
            }

    except Exception as e:
        print(f"❌ Erro no {ticker}: {e}")

    return None


# =========================================
# 🚀 EXECUÇÃO PRINCIPAL
# =========================================

def main():
    print("🚀 Iniciando coleta de detalhes...")

    session = criar_sessao()
    df_tickers = carregar_tickers()

    dados = []

    for idx, row in df_tickers.iterrows():
        ticker = row["ticker"]

        print(f"🔎 {idx+1}/{len(df_tickers)} - {ticker}")

        resultado = extrair_dados_fii(session, ticker)

        if resultado:
            dados.append(resultado)

        time.sleep(2)  # evita bloqueio

    df = pd.DataFrame(dados)

    # =========================================
# 💾 SALVAR SNAPSHOT + HISTÓRICO MENSAL
# =========================================

def salvar_arquivos(df):
    hoje = datetime.now()

    # 📌 arquivo snapshot (sempre o último)
    arquivo_atual = "fiis_detalhes_atual.csv"

    # 📌 arquivo mensal
    arquivo_mensal = f"fiis_detalhes_{hoje.strftime('%Y%m')}.csv"

    # =========================
    # 1. SALVAR SNAPSHOT
    # =========================
    df.to_csv(
        arquivo_atual,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig"
    )

    # =========================
    # 2. ATUALIZAR HISTÓRICO
    # =========================
    if os.path.exists(arquivo_mensal):
        df_hist = pd.read_csv(arquivo_mensal, sep=";", decimal=",")

        df_total = pd.concat([df_hist, df], ignore_index=True)

        # 🔥 remove duplicados do mesmo dia/ticker
        df_total = df_total.drop_duplicates(
            subset=["data_coleta", "ticker"],
            keep="last"
        )
    else:
        df_total = df

    df_total.to_csv(
        arquivo_mensal,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig"
    )

    print(f"📁 Snapshot salvo: {arquivo_atual}")
    print(f"📁 Histórico mensal: {arquivo_mensal}")


# =========================================
# ▶️ RODAR
# =========================================

if __name__ == "__main__":
    main()