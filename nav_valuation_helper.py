import os
import asyncio
from typing import Dict, List, Optional

import pandas as pd
import config
from streamlit import cache

# icom_price.py (adapte le module si besoin)
from icom_price import loadedbasketbulk, get_pose_price, ftAssetCode


class OverlapCore:
    def __init__(self, etf_1: str) -> None:
        self.etf_1 = etf_1

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _strip_equity(bbg_ticker: Optional[str]) -> Optional[str]:
        """
        ftAssetCode renvoie typiquement: 'EUN5 GY Equity'
        On veut: 'EUN5 GY'
        """
        if not bbg_ticker:
            return None
        t = str(bbg_ticker).strip()
        if not t:
            return None
        if t.endswith(" Equity"):
            t = t[: -len(" Equity")].strip()
        return t

    @staticmethod
    def _as_equity(ticker_no_equity: str) -> str:
        """Format attendu par loadedbasketbulk."""
        t = (ticker_no_equity or "").strip()
        return t if t.endswith("Equity") else f"{t} Equity"

    @staticmethod
    def _load_etf_infos() -> pd.DataFrame:
        df = pd.read_csv(config.FILE_ETF_INFOS)
        df.columns = [c.strip() for c in df.columns]
        return df

    @staticmethod
    def _mnemo_to_isin_map() -> Dict[str, str]:
        """
        Map mnemo -> ISIN à partir de config.FILE_ETF_INFOS
        Colonnes attendues: ETF_TICKER, ISIN
        """
        df = OverlapCore._load_etf_infos()
        df2 = df[["ETF_TICKER", "ISIN"]].dropna().drop_duplicates()
        df2["ETF_TICKER"] = df2["ETF_TICKER"].astype(str).str.strip()
        df2["ISIN"] = df2["ISIN"].astype(str).str.strip()
        return dict(zip(df2["ETF_TICKER"], df2["ISIN"]))

    @staticmethod
    def _resolve_bbg_from_isin(isin: str) -> Optional[str]:
        """
        Résout ISIN -> ticker BBG complet via ftAssetCode, puis supprime 'Equity'.
        Exemple:
          ftAssetCode("ISIN", "IE00B3F81R35", "BBG") -> "EUN5 GY Equity"
          => "EUN5 GY"
        """
        try:
            full = ftAssetCode("ISIN", isin, "BBG")
        except Exception:
            return None
        return OverlapCore._strip_equity(full)

    @staticmethod
    def _get_pose_for_mnemos(mnemos: List[str]) -> Dict[str, float]:
        """
        Pour chaque mnemo (ex: 'ECRP'), récupère l'ISIN (FILE_ETF_INFOS),
        puis ticker BBG via ftAssetCode, puis POSE via get_pose_price.
        """
        mnemo_isin = OverlapCore._mnemo_to_isin_map()

        # 1) mnemo -> resolved BBG ticker (sans Equity)
        resolved: Dict[str, Optional[str]] = {}
        for m in mnemos:
            isin = mnemo_isin.get(m)
            resolved[m] = OverlapCore._resolve_bbg_from_isin(isin) if isin else None

        tickers_no_equity = [t for t in resolved.values() if t]
        if not tickers_no_equity:
            return {m: float("nan") for m in mnemos}

        # 2) loadedbasketbulk a besoin de "Ticker Listing Equity"
        bbg_equity = [OverlapCore._as_equity(t) for t in tickers_no_equity]

        mx_names = loadedbasketbulk("BBG", bbg_equity, "MX_Name")
        _ = loadedbasketbulk("BBG", bbg_equity, "Currency")  # optionnel, mais demandé initialement

        # 3) NMP format (corrigé): "NMP." + mx.replace(" ", "_")
        sec_to_nmp: Dict[str, str] = {}
        for sec in bbg_equity:
            mx = mx_names.get(sec)
            if mx:
                sec_to_nmp[sec] = "NMP." + str(mx).replace(" ", "_")

        if not sec_to_nmp:
            return {m: float("nan") for m in mnemos}

        # 4) get_pose_price (async) -> pose dict
        pose_price = asyncio.run(get_pose_price(list(sec_to_nmp.values())))
        pose_dict = pose_price[0] if pose_price and len(pose_price) > 0 else {}

        # 5) map back mnemo -> pose
        out: Dict[str, float] = {}
        for m, t_no_eq in resolved.items():
            if not t_no_eq:
                out[m] = float("nan")
                continue

            sec = OverlapCore._as_equity(t_no_eq)
            nmp = sec_to_nmp.get(sec)
            out[m] = pose_dict.get(nmp, float("nan")) if nmp else float("nan")

        return out

    # ---------------------------------------------------------------------
    # Existing methods
    # ---------------------------------------------------------------------
    def map_index_to_etf(self, tracked_index: str) -> str:
        df = pd.read_csv(config.FILE_ETF_INFOS)
        df_final = df[["Tracked index", "ETF_TICKER"]]
        df_final.set_index("Tracked index", inplace=True)
        df_final.drop_duplicates(inplace=True)
        return df_final.loc[tracked_index].values[0]

    @cache
    def get_top_5_overlap(self) -> pd.DataFrame:
        df_ori = pd.read_csv(config.FILE_OVERLAP_MATRIX, index_col=0)
        s = df_ori.loc[self.etf_1].sort_values(ascending=False)
        return pd.DataFrame(s)

    @staticmethod
    def get_all_etf() -> List[str]:
        with open(os.path.join(config.FOLDER_PROJECT_TEAM, "Overlap", "list_etfs.txt"), "r", encoding="utf-8") as f:
            return [x.strip() for x in f.read().splitlines() if x.strip()]

    # ---------------------------------------------------------------------
    # Modified: adds POSE
    # ---------------------------------------------------------------------
    def getOverlapFinal(self) -> pd.DataFrame:
        compo_matrix = pd.read_csv(config.FILE_COMPO_MATRIX)

        compo = pd.read_csv(
            os.path.join(config.FOLDER_ICOM_PRICING, f"{self.etf_1}.csv"),
            usecols=["Instrument", "ProductClass"],
        )
        bonds = compo[compo["ProductClass"] == "Bond"]

        compo_matrix_filtered = compo_matrix.loc[
            compo_matrix["Instrument"].isin(bonds["Instrument"])
        ].copy()

        if "Unnamed: 0" in compo_matrix_filtered.columns:
            compo_matrix_filtered.drop("Unnamed: 0", axis=1, inplace=True)

        compo_matrix_filtered.fillna(0, inplace=True)
        compo_matrix_filtered.drop("Instrument", axis=1, inplace=True)

        overlap_dict = {
            other_ticker: compo_matrix_filtered[[self.etf_1, other_ticker]].min(axis=1).sum()
            for other_ticker in compo_matrix_filtered.columns
            if other_ticker != self.etf_1
        }

        result_df = pd.DataFrame.from_dict(overlap_dict, orient="index", columns=[self.etf_1])
        final_overlap = result_df.sort_values(by=self.etf_1, ascending=False)

        # NEW: add POSE column
        pose_map = self._get_pose_for_mnemos(final_overlap.index.tolist())
        final_overlap["POSE"] = final_overlap.index.map(lambda x: pose_map.get(x, float("nan")))

        return final_overlap
