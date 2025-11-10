#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
find_unresolved_tokens.py

Lee 3 CSV limpios (zenodo/icpsr/yanmaksi), intenta resolver cada token por symbol y/o name
en CoinGecko, CoinMarketCap y CryptoTotem. Exporta un CSV con los que NO se encuentran
(en ninguno) o los que tienen symbol/ticker vacío/nulo, indicando el dataset de origen.

Uso:
  python scripts/find_unresolved_tokens.py \
    --zenodo data/processed/zenodo_fahlenbrach_clean.csv \
    --icpsr  data/processed/icpsr_villanueva_clean.csv \
    --yan    data/processed/kaggle_yanmaksi_clean.csv \
    --out    data/processed/unresolved_tokens.csv \
    --cmc-key YOUR_CMC_API_KEY

Notas:
- CoinGecko no requiere API key.
- CoinMarketCap requiere API key; también puede pasarse por env: CMC_API_KEY
- CryptoTotem no tiene API; se hace un GET de búsqueda best-effort.

"""

import os
import re
import time
import argparse
import requests
import pandas as pd
from difflib import SequenceMatcher

# ---------------------------
# Config
# ---------------------------
USER_AGENT = "Mozilla/5.0 (compatible; TFM-ICO-Resolver/1.0; +https://example.local)"
CG_LIST_URL = "https://api.coingecko.com/api/v3/coins/list?include_platform=false"
CG_SEARCH_URL = "https://api.coingecko.com/api/v3/search?query={q}"
CMC_MAP_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
CMC_INFO_URL = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
CTOTEM_SEARCH_URL = "https://cryptototem.com/?s={q}"

HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# pausa entre requests para no quemar endpoints
SLEEP_CG = 0.3
SLEEP_CMC = 0.4
SLEEP_CT = 0.8

# umbral de similitud para nombres
NAME_FUZZY_THRESHOLD = 0.88

# ---------------------------
# Helpers
# ---------------------------
def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def safe_get(d, k, default=None):
    try:
        return d.get(k, default)
    except Exception:
        return default

# ---------------------------
# CoinGecko resolvers
# ---------------------------
class CoinGeckoResolver:
    def __init__(self):
        self._coins_list = None
        self._by_symbol = {}
        self._by_name = {}

    def _load_list(self):
        if self._coins_list is None:
            r = requests.get(CG_LIST_URL, headers=HEADERS, timeout=30)
            r.raise_for_status()
            self._coins_list = r.json()  # list of {id, symbol, name}
            # index
            for c in self._coins_list:
                sym = normalize_text(c.get("symbol"))
                nam = normalize_text(c.get("name"))
                if sym:
                    self._by_symbol.setdefault(sym, []).append(c)
                if nam:
                    self._by_name.setdefault(nam, []).append(c)
            time.sleep(SLEEP_CG)

    def find(self, symbol: str, name: str):
        """
        Devuelve dict:
          {"found": bool, "cg_id": str|None, "cg_symbol": str|None, "cg_name": str|None, "method": "symbol|name|search|none"}
        """
        self._load_list()
        sym_std = normalize_text(symbol)
        nam_std = normalize_text(name)

        # 1) match exacto por symbol
        if sym_std and sym_std in self._by_symbol:
            # si hay many, intentar desempatar por name fuzzy
            cand = self._by_symbol[sym_std]
            if nam_std:
                best = max(cand, key=lambda c: similar(nam_std, normalize_text(c.get("name"))))
            else:
                best = cand[0]
            return {
                "found": True,
                "cg_id": best.get("id"),
                "cg_symbol": best.get("symbol"),
                "cg_name": best.get("name"),
                "method": "symbol",
            }

        # 2) exacto por name
        if nam_std and nam_std in self._by_name:
            best = self._by_name[nam_std][0]
            return {
                "found": True,
                "cg_id": best.get("id"),
                "cg_symbol": best.get("symbol"),
                "cg_name": best.get("name"),
                "method": "name",
            }

        # 3) fallback: CG search
        if symbol or name:
            q = symbol or name
            url = CG_SEARCH_URL.format(q=requests.utils.quote(q))
            try:
                r = requests.get(url, headers=HEADERS, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    coins = data.get("coins", [])
                    if coins:
                        # elegir el más parecido por nombre si tenemos name, sino por symbol
                        if name:
                            nam_std = normalize_text(name)
                            best = max(
                                coins,
                                key=lambda c: similar(nam_std, normalize_text(c.get("name")))
                            )
                        else:
                            sym_std = normalize_text(symbol)
                            best = max(
                                coins,
                                key=lambda c: similar(sym_std, normalize_text(c.get("symbol")))
                            )
                        time.sleep(SLEEP_CG)
                        return {
                            "found": True,
                            "cg_id": best.get("id"),
                            "cg_symbol": best.get("symbol"),
                            "cg_name": best.get("name"),
                            "method": "search",
                        }
            except Exception:
                pass
            time.sleep(SLEEP_CG)

        return {"found": False, "cg_id": None, "cg_symbol": None, "cg_name": None, "method": "none"}

# ---------------------------
# CoinMarketCap resolver
# ---------------------------
class CMCResolver:
    def __init__(self, api_key: str | None):
        self.api_key = api_key
        self.headers = {**HEADERS, "X-CMC_PRO_API_KEY": api_key} if api_key else HEADERS
        self.cache_symbol = {}
        self.cache_name = {}

    def find(self, symbol: str, name: str):
        if not self.api_key:
            return {"found": False, "cmc_id": None, "cmc_symbol": None, "cmc_name": None, "method": "no_api_key"}

        sym = (symbol or "").strip()
        nam = (name or "").strip()

        # 1) map por symbol (rápido)
        if sym:
            key = sym.upper()
            if key in self.cache_symbol:
                return self.cache_symbol[key]
            try:
                r = requests.get(
                    CMC_MAP_URL,
                    headers=self.headers,
                    params={"symbol": key},
                    timeout=30,
                )
                if r.status_code == 200:
                    data = r.json()
                    arr = data.get("data", []) or []
                    if arr:
                        # elegir el más antiguo / activo
                        best = arr[0]
                        out = {
                            "found": True,
                            "cmc_id": best.get("id"),
                            "cmc_symbol": best.get("symbol"),
                            "cmc_name": best.get("name"),
                            "method": "symbol",
                        }
                        self.cache_symbol[key] = out
                        time.sleep(SLEEP_CMC)
                        return out
            except Exception:
                pass
            time.sleep(SLEEP_CMC)

        # 2) map por nombre (menos confiable)
        if nam:
            key = nam.lower()
            if key in self.cache_name:
                return self.cache_name[key]
            try:
                r = requests.get(
                    CMC_MAP_URL,
                    headers=self.headers,
                    params={"listing_status": "active,inactive,untracked", "aux": "name,symbol,slug"},
                    timeout=30,
                )
                if r.status_code == 200:
                    data = r.json()
                    arr = data.get("data", []) or []
                    # fuzzy por nombre
                    nam_std = normalize_text(nam)
                    if arr:
                        best = max(arr, key=lambda c: similar(nam_std, normalize_text(c.get("name"))))
                        if similar(nam_std, normalize_text(best.get("name"))) >= NAME_FUZZY_THRESHOLD:
                            out = {
                                "found": True,
                                "cmc_id": best.get("id"),
                                "cmc_symbol": best.get("symbol"),
                                "cmc_name": best.get("name"),
                                "method": "name_fuzzy",
                            }
                            self.cache_name[key] = out
                            time.sleep(SLEEP_CMC)
                            return out
            except Exception:
                pass
            time.sleep(SLEEP_CMC)

        return {"found": False, "cmc_id": None, "cmc_symbol": None, "cmc_name": None, "method": "none"}

# ---------------------------
# CryptoTotem (best-effort)
# ---------------------------
def cryptototem_exists(query: str) -> bool:
    if not query:
        return False
    url = CTOTEM_SEARCH_URL.format(q=requests.utils.quote(query))
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        if r.status_code != 200:
            return False
        # heurística: si aparecen tarjetas de proyecto
        html = r.text.lower()
        # buscar patrones comunes en listados de resultados
        # (ligero para no depender de clases específicas)
        if "ico" in html and "html" in html:
            # más simple: si devuelve resultados con "/ico/" en links
            return "/ico/" in html
    except Exception:
        return False
    finally:
        time.sleep(SLEEP_CT)
    return False

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zenodo", required=True, help="CSV limpio de Zenodo/Fahlenbrach")
    ap.add_argument("--icpsr", required=True, help="CSV limpio de ICPSR/Villanueva")
    ap.add_argument("--yan",   required=True, help="CSV limpio de Kaggle YanMaksi")
    ap.add_argument("--out",   required=True, help="CSV salida con tokens no resueltos")
    ap.add_argument("--cmc-key", default=os.getenv("CMC_API_KEY"), help="API key de CoinMarketCap (o setear env CMC_API_KEY)")
    args = ap.parse_args()

    # Carga datasets
    inputs = {
        "zenodo_fahlenbrach": pd.read_csv(args.zenodo),
        "icpsr_villanueva":   pd.read_csv(args.icpsr),
        "kaggle_yanmaksi":    pd.read_csv(args.yan),
    }

    # Normalizar headers
    for k in inputs:
        df = inputs[k]
        df.columns = df.columns.str.strip().str.lower()
        inputs[k] = df

    # Detectar columnas de symbol/name estándar
    def pick(df, cols):
        for c in cols:
            if c in df.columns:
                return c
        return None

    # Inicializar resolvers
    cg = CoinGeckoResolver()
    cmc = CMCResolver(api_key=args.cmc_key)

    rows_out = []

    for ds_name, df in inputs.items():
        # elegir columnas
        sym_col = pick(df, ["symbol_std", "symbol", "ticker", "ticker_symbol"])
        name_col = pick(df, ["name_std", "name", "project name", "project_name", "ico_name"])

        # fallback si no hay nada
        if not sym_col and "coin_ticker" in df.columns:
            sym_col = "coin_ticker"
        if not name_col and "coin_ticker" in df.columns:
            name_col = "coin_ticker"

        # recorrer filas
        for idx, row in df.iterrows():
            raw_sym = row.get(sym_col) if sym_col else None
            raw_name = row.get(name_col) if name_col else None

            sym = str(raw_sym) if pd.notna(raw_sym) else ""
            nam = str(raw_name) if pd.notna(raw_name) else ""

            sym_std = normalize_text(sym)
            nam_std = normalize_text(nam)

            # caso: symbol vacío/nulo -> reportar directo
            if not sym_std:
                rows_out.append({
                    "source_dataset": ds_name,
                    "row_index": idx,
                    "symbol": sym,
                    "name": nam,
                    "coingecko_found": False,
                    "cmc_found": False,
                    "cryptototem_found": False,
                    "note": "missing_symbol",
                })
                continue

            # 1) CoinGecko
            cg_res = cg.find(symbol=sym, name=nam)
            if cg_res["found"]:
                # resuelto en CG -> no se agrega a 'unresolved'
                print(f"Token {sym} resuelto en CoinGecko.")
                continue

            # 2) CoinMarketCap
            cmc_res = cmc.find(symbol=sym, name=nam)
            if cmc_res["found"]:
                print(f"Token {sym} resuelto en CoinMarketCap.")
                continue

            # 3) CryptoTotem (best effort): probar con symbol y con name si existiera
            ct_found = cryptototem_exists(sym) or (cryptototem_exists(nam) if nam else False)

            if not ct_found:
                print(f"Token {sym} no encontrado en ningun lado.")
                rows_out.append({
                    "source_dataset": ds_name,
                    "row_index": idx,
                    "symbol": sym,
                    "name": nam,
                    "coingecko_found": False,
                    "cmc_found": False if args.cmc_key else "skipped_no_key",
                    "cryptototem_found": False,
                    "note": "not_found_anywhere",
                })
            else:
                print(f"Token {sym} resuelto en CryptoTotem.")

    out_df = pd.DataFrame(rows_out, columns=[
        "source_dataset", "row_index", "symbol", "name",
        "coingecko_found", "cmc_found", "cryptototem_found", "note"
    ])
    out_df.to_csv(args.out, index=False)
    print(f"✅ Guardado: {args.out} ({len(out_df)} filas)")

if __name__ == "__main__":
    main()
