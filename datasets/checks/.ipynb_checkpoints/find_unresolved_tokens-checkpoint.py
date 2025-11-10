#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
find_unresolved_tokens.py

Lee 3 CSV limpios (zenodo/icpsr/yanmaksi), intenta resolver cada token por symbol y/o name
en CoinGecko, CoinMarketCap, CoinPaprika y Foundico. Exporta un CSV con los que NO se encuentran
(en ninguno) o los que tienen symbol/ticker vacío/nulo, indicando el dataset de origen.

Uso:
  python scripts/find_unresolved_tokens.py \
    --zenodo data/processed/zenodo_fahlenbrach_clean.csv \
    --icpsr  data/processed/icpsr_villanueva_clean.csv \
    --yan    data/processed/kaggle_yanmaksi_clean.csv \
    --out    data/processed/unresolved_tokens.csv \
    --cmc-key 87f241ea-b56c-4a2f-9707-e25b4352ceb6 \
    --foundico-public YOUR_PUBLIC \
    --foundico-private YOUR_PRIVATE \
    --foundico-max-pages 15

Uso PowerShell:
$env:CMC_API_KEY="tu_api_key_cmc"
$env:FOUNDICO_PUBLIC_KEY="tu_public_key"
$env:FOUNDICO_PRIVATE_KEY="tu_private_key"

python scripts/find_unresolved_tokens.py ^
  --zenodo data/processed/zenodo_fahlenbrach_clean.csv ^
  --icpsr  data/processed/icpsr_villanueva_clean.csv ^
  --yan    data/processed/kaggle_yanmaksi_clean.csv ^
  --out    data/processed/unresolved_tokens.csv ^
  --foundico-max-pages 20

Notas:
- CoinGecko: sin API key.
- CoinPaprika: plan free no requiere API key (tiene rate limits).
- CoinMarketCap: requiere API key (o env CMC_API_KEY).
- Foundico: requiere PUBLIC y PRIVATE key (o env FOUNDICO_PUBLIC_KEY y FOUNDICO_PRIVATE_KEY).
"""

import os
import re
import hmac
import base64
import hashlib
import time
import argparse
import requests
import pandas as pd
from difflib import SequenceMatcher

# ---------------------------
# Config
# ---------------------------
USER_AGENT = "Mozilla/5.0 (compatible; TFM-ICO-Resolver/1.1; +https://example.local)"
HEADERS_JSON = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# CoinGecko
CG_LIST_URL   = "https://api.coingecko.com/api/v3/coins/list?include_platform=false"
CG_SEARCH_URL = "https://api.coingecko.com/api/v3/search?query={q}"
SLEEP_CG  = 0.25

# CoinMarketCap
CMC_MAP_URL  = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
SLEEP_CMC = 0.35

# CoinPaprika (search)
# Docs: https://docs.coinpaprika.com/api-reference/tools/search  (GET /v1/search?q=...)
CPK_SEARCH_URL = "https://api.coinpaprika.com/v1/search"
SLEEP_CPK = 0.3

# Foundico (POST + firma HMAC-SHA256 del JSON body, encabezados X-Foundico-*)
# Docs: https://foundico.com/developers/
FD_BASE = "https://foundico.com/api/v1"
FD_ICOS = f"{FD_BASE}/icos/"
FD_ICO  = f"{FD_BASE}/ico/"
SLEEP_FD = 0.45

# Fuzzy nombre
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

def sign_foundico(private_key: str, payload_json: str) -> str:
    """
    Firma HMAC-SHA256 y la devuelve en base64 (binario → base64),
    como requiere 'X-Foundico-Access-Key'.
    """
    mac = hmac.new(
        private_key.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256
    ).digest()
    return base64.b64encode(mac).decode("utf-8")

def post_foundico(url: str, public_key: str, private_key: str, payload: dict):
    body = pd.io.json.dumps(payload, ensure_ascii=False)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "X-Foundico-Public-Key": public_key,
        "X-Foundico-Access-Key": sign_foundico(private_key, body),
    }
    r = requests.post(url, headers=headers, data=body, timeout=30)
    return r

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
            r = requests.get(CG_LIST_URL, headers=HEADERS_JSON, timeout=30)
            r.raise_for_status()
            self._coins_list = r.json()  # [{id,symbol,name}, ...]
            for c in self._coins_list:
                sym = normalize_text(c.get("symbol"))
                nam = normalize_text(c.get("name"))
                if sym:
                    self._by_symbol.setdefault(sym, []).append(c)
                if nam:
                    self._by_name.setdefault(nam, []).append(c)
            time.sleep(SLEEP_CG)

    def find(self, symbol: str, name: str):
        self._load_list()
        sym_std = normalize_text(symbol)
        nam_std = normalize_text(name)

        # 1) exact symbol
        if sym_std and sym_std in self._by_symbol:
            cand = self._by_symbol[sym_std]
            if nam_std:
                best = max(cand, key=lambda c: similar(nam_std, normalize_text(c.get("name"))))
            else:
                best = cand[0]
            return {"found": True, "cg_id": best.get("id"), "cg_symbol": best.get("symbol"),
                    "cg_name": best.get("name"), "method": "symbol"}

        # 2) exact name
        if nam_std and nam_std in self._by_name:
            best = self._by_name[nam_std][0]
            return {"found": True, "cg_id": best.get("id"), "cg_symbol": best.get("symbol"),
                    "cg_name": best.get("name"), "method": "name"}

        # 3) search fallback
        q = symbol or name
        if q:
            url = CG_SEARCH_URL.format(q=requests.utils.quote(q))
            try:
                r = requests.get(url, headers=HEADERS_JSON, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    coins = data.get("coins", [])
                    if coins:
                        if name:
                            nam_std = normalize_text(name)
                            best = max(coins, key=lambda c: similar(nam_std, normalize_text(c.get("name"))))
                        else:
                            sym_std = normalize_text(symbol)
                            best = max(coins, key=lambda c: similar(sym_std, normalize_text(c.get("symbol"))))
                        time.sleep(SLEEP_CG)
                        return {"found": True, "cg_id": best.get("id"), "cg_symbol": best.get("symbol"),
                                "cg_name": best.get("name"), "method": "search"}
            except Exception:
                pass
            time.sleep(SLEEP_CG)

        return {"found": False}

# ---------------------------
# CoinMarketCap resolver
# ---------------------------
class CMCResolver:
    def __init__(self, api_key: str | None):
        self.api_key = api_key
        self.headers = {**HEADERS_JSON, "X-CMC_PRO_API_KEY": api_key} if api_key else HEADERS_JSON
        self.cache_symbol = {}
        self.cache_name = {}

    def find(self, symbol: str, name: str):
        if not self.api_key:
            return {"found": False, "method": "no_api_key"}

        sym = (symbol or "").strip()
        nam = (name or "").strip()

        # 1) map by symbol
        if sym:
            key = sym.upper()
            if key in self.cache_symbol:
                return self.cache_symbol[key]
            try:
                r = requests.get(CMC_MAP_URL, headers=self.headers, params={"symbol": key}, timeout=30)
                if r.status_code == 200:
                    arr = (r.json().get("data") or [])
                    if arr:
                        best = arr[0]
                        out = {"found": True, "cmc_id": best.get("id"),
                               "cmc_symbol": best.get("symbol"), "cmc_name": best.get("name"),
                               "method": "symbol"}
                        self.cache_symbol[key] = out
                        time.sleep(SLEEP_CMC)
                        return out
            except Exception:
                pass
            time.sleep(SLEEP_CMC)

        # 2) fuzzy by name (heavy; we avoid full dump, so we skip here or require symbol)
        if nam:
            # NOTE: CMC map sin filtro por name directo; podrías implementar un caché local de ids,
            # pero para mantener costo bajo omitimos name-only salvo symbol faltante.
            return {"found": False, "method": "name_not_supported"}

        return {"found": False}

# ---------------------------
# CoinPaprika resolver
# ---------------------------
class CoinPaprikaResolver:
    """
    Usa /v1/search?q=... y prioriza currencies; soporta modifier=symbol_search para buscar por símbolo exacto.
    """
    def __init__(self):
        self.base = CPK_SEARCH_URL

    def find(self, symbol: str, name: str):
        sym = (symbol or "").strip()
        nam = (name or "").strip()

        # 1) symbol search (estricto en currencies)
        if sym:
            try:
                r = requests.get(self.base, headers=HEADERS_JSON,
                                 params={"q": sym, "c": "currencies", "modifier": "symbol_search", "limit": 10},
                                 timeout=30)
                if r.status_code == 200:
                    data = r.json() or {}
                    cur = data.get("currencies") or []
                    if cur:
                        # exact symbol match preferido
                        sym_std = normalize_text(sym)
                        exact = [x for x in cur if normalize_text(x.get("symbol")) == sym_std]
                        best = exact[0] if exact else cur[0]
                        time.sleep(SLEEP_CPK)
                        return {"found": True, "cpk_id": best.get("id"),
                                "cpk_symbol": best.get("symbol"), "cpk_name": best.get("name"),
                                "method": "symbol_search"}
            except Exception:
                pass
            time.sleep(SLEEP_CPK)

        # 2) name search
        if nam:
            try:
                r = requests.get(self.base, headers=HEADERS_JSON,
                                 params={"q": nam, "c": "currencies,icos", "limit": 20},
                                 timeout=30)
                if r.status_code == 200:
                    data = r.json() or {}
                    cand = (data.get("currencies") or []) + (data.get("icos") or [])
                    if cand:
                        nam_std = normalize_text(nam)
                        best = max(cand, key=lambda x: similar(nam_std, normalize_text(x.get("name"))))
                        if similar(nam_std, normalize_text(best.get("name"))) >= 0.80:
                            time.sleep(SLEEP_CPK)
                            return {"found": True, "cpk_id": best.get("id"),
                                    "cpk_symbol": best.get("symbol"), "cpk_name": best.get("name"),
                                    "method": "name_fuzzy"}
            except Exception:
                pass
            time.sleep(SLEEP_CPK)

        return {"found": False}

# ---------------------------
# Foundico resolver (paginado + fuzzy/name + exact/symbol)
# ---------------------------
class FoundicoResolver:
    """
    Requiere claves. Hace POST a /api/v1/icos/ con status='past' y pagina 'page'.
    Busca por:
      - match exacto de finance.ticker (símbolo),
      - o fuzzy por main.name (nombre).
    """
    def __init__(self, public_key: str | None, private_key: str | None, max_pages: int = 15):
        self.public = public_key
        self.private = private_key
        self.max_pages = max_pages
        self.enabled = bool(public_key and private_key)

    def find(self, symbol: str, name: str):
        if not self.enabled:
            return {"found": False, "method": "foundico_disabled"}
        sym_std = normalize_text(symbol)
        nam_std = normalize_text(name)

        for page in range(1, self.max_pages + 1):
            payload = {"status": "past", "page": page}
            r = post_foundico(FD_ICOS, self.public, self.private, payload)
            if r.status_code != 200:
                # parar si error auth/limits
                return {"found": False, "method": f"foundico_http_{r.status_code}"}
            data = r.json() or {}
            items = data.get("data") or []
            if not items:
                break

            # scan de la página
            best_name = None
            best_score = 0.0
            for item in items:
                main = item.get("main") or {}
                finance = item.get("finance") or {}
                name_i = (main.get("name") or "").strip()
                sym_i  = (finance.get("ticker") or "").strip()
                if sym_std and normalize_text(sym_i) == sym_std:
                    # match por símbolo: resolvimos
                    return {"found": True, "fd_id": item.get("id"), "fd_name": name_i,
                            "fd_symbol": sym_i, "fd_url": (item.get("links") or {}).get("url"),
                            "method": "symbol_exact"}
                if nam_std:
                    score = similar(nam_std, normalize_text(name_i))
                    if score > best_score:
                        best_score, best_name = score, {"fd_id": item.get("id"),
                                                        "fd_name": name_i,
                                                        "fd_symbol": sym_i,
                                                        "fd_url": (item.get("links") or {}).get("url")}
            if best_name and best_score >= NAME_FUZZY_THRESHOLD:
                return {"found": True, **best_name, "method": "name_fuzzy"}

            time.sleep(SLEEP_FD)

        return {"found": False}

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zenodo", required=True, help="CSV limpio de Zenodo/Fahlenbrach")
    ap.add_argument("--icpsr", required=True, help="CSV limpio de ICPSR/Villanueva")
    ap.add_argument("--yan",   required=True, help="CSV limpio de Kaggle YanMaksi")
    ap.add_argument("--out",   required=True, help="CSV salida con tokens no resueltos")

    ap.add_argument("--cmc-key", default=os.getenv("CMC_API_KEY"), help="API key de CoinMarketCap (o env CMC_API_KEY)")
    ap.add_argument("--foundico-public",  default=os.getenv("FOUNDICO_PUBLIC_KEY"),  help="Foundico PUBLIC key (o env FOUNDICO_PUBLIC_KEY)")
    ap.add_argument("--foundico-private", default=os.getenv("FOUNDICO_PRIVATE_KEY"), help="Foundico PRIVATE key (o env FOUNDICO_PRIVATE_KEY)")
    ap.add_argument("--foundico-max-pages", type=int, default=15, help="Cantidad máxima de páginas a paginar en Foundico (/icos/).")

    args = ap.parse_args()

    # Carga datasets
    inputs = {
        "zenodo_fahlenbrach": pd.read_csv(args.zenodo),
        "icpsr_villanueva":   pd.read_csv(args.icpsr),
        "kaggle_yanmaksi":    pd.read_csv(args.yan),
    }
    for k in inputs:
        df = inputs[k]
        df.columns = df.columns.str.strip().str.lower()

    # Detectar columnas
    def pick(df, cols):
        for c in cols:
            if c in df.columns:
                return c
        return None

    # Inicializar resolvers
    cg  = CoinGeckoResolver()
    cmc = CMCResolver(api_key=args.cmc_key)
    cpk = CoinPaprikaResolver()
    fdi = FoundicoResolver(public_key=args.foundico_public, private_key=args.foundico_private,
                           max_pages=args.foundico_max_pages)

    rows_out = []

    for ds_name, df in inputs.items():
        sym_col  = pick(df, ["symbol_std", "symbol", "ticker", "ticker_symbol", "coin_ticker"])
        name_col = pick(df, ["name_std", "name", "project name", "project_name", "ico_name"])
        if not sym_col and "coin_ticker" in df.columns:
            sym_col = "coin_ticker"
        if not name_col and "coin_ticker" in df.columns:
            name_col = "coin_ticker"

        for idx, row in df.iterrows():
            sym = str(row.get(sym_col)) if (sym_col and pd.notna(row.get(sym_col))) else ""
            nam = str(row.get(name_col)) if (name_col and pd.notna(row.get(name_col))) else ""

            sym_std = normalize_text(sym)
            nam_std = normalize_text(nam)

            # Símbolo vacío → directo a salida
            if not sym_std:
                rows_out.append({
                    "source_dataset": ds_name,
                    "row_index": idx,
                    "symbol": sym,
                    "name": nam,
                    "coingecko_found": False,
                    "cmc_found": False,
                    "coinpaprika_found": False,
                    "foundico_found": False,
                    "note": "missing_symbol",
                })
                continue

            # 1) CoinGecko
            cg_res = cg.find(symbol=sym, name=nam)
            if cg_res.get("found"):
                continue

            # 2) CoinMarketCap
            cmc_res = cmc.find(symbol=sym, name=nam)
            if cmc_res.get("found"):
                continue

            # 3) CoinPaprika
            cpk_res = cpk.find(symbol=sym, name=nam)
            if cpk_res.get("found"):
                continue

            # 4) Foundico (si hay claves)
            fdi_res = fdi.find(symbol=sym, name=nam)
            if fdi_res.get("found"):
                continue

            # Si no se encontró en ninguno
            rows_out.append({
                "source_dataset": ds_name,
                "row_index": idx,
                "symbol": sym,
                "name": nam,
                "coingecko_found": False,
                "cmc_found": False if args.cmc_key else "skipped_no_key",
                "coinpaprika_found": False,
                "foundico_found": False if fdi.enabled else "skipped_no_keys",
                "note": "not_found_anywhere",
            })

    out_df = pd.DataFrame(rows_out, columns=[
        "source_dataset", "row_index", "symbol", "name",
        "coingecko_found", "cmc_found", "coinpaprika_found", "foundico_found", "note"
    ])
    out_df.to_csv(args.out, index=False)
    print(f"✅ Guardado: {args.out} ({len(out_df)} filas)")

if __name__ == "__main__":
    main()
