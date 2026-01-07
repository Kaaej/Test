import os
import asyncio
from typing import Dict, List, Optional

import pandas as pd
import config
import streamlit as st
from streamlit import cache

# icom_price.py
from icom_price import loadedbasketbulk, get_pose_price, ftassetcode


class OverlapCore:
    def __init__(self, etf_1: str) -> None:
        self.etf_1 = etf_1

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def _strip_equity(bbg_ticker: Optional[str]) -> Optional[str]:
        if not bbg_ticker:
            return None
        t = str(bbg_ticker).strip()
        if t.endswith(" Equity"):
            t = t[: -len(" Equity")].strip()
        return t

    @staticmethod
    def _as_equity(ticker_no_equity: str) -> str:
        return f"{ticker_no_equity} Equity"

    @staticmethod
    def _load_etf_infos() -> pd.DataFrame:
        df = pd.read_csv(config.FILE_ETF_INFOS)
        df.columns = [c.strip() for c in df.columns]
        return df

    @staticmethod
    def _mnemo_to_isin_map() -> Dict[str, str]:
        df = OverlapCore._load_etf_infos()
        df2 = df[["ETF_TICKER", "ISIN"]].dropna().drop_duplicates()
        df2["ETF_TICKER"] = df2["ETF_TICKER"].astype(str).str.strip()
        df2["ISIN"] = df2["ISIN"].astype(str).str.strip()
        return dict(zip(df2["ETF_TICKER"], df2["ISIN"]))

    # ---------------------------------------------------------------------
    # ISIN -> BBG ticker
    # ---------------------------------------------------------------------
    @staticmethod
    def _resolve_bbg_from_isin(isin: str) -> Optional[str]:
        try:
            full = ftassetcode("ISIN", isin, "BBG")
        except Exception:
            return None
        return OverlapCore._strip_equity(full)

    # ---------------------------------------------------------------------
    # POSE
    # ---------------------------------------------------------------------
    @staticmethod
    def _get_pose_for_mnemos(mnemos: List[str]) -> Dict[str, float]:
        mnemo_isin = OverlapCore._mnemo_to_isin_map()

        st.write("🔎 Résolution ISIN → ticker Bloomberg…")
        progress = st.progress(0)

        # 1) mnemo -> BBG ticker
        resolved: Dict[str, Optional[str]] = {}
        for i, m in enumerate(mnemos, start=1):
            isin = mnemo_isin.get(m)
            resolved[m] = OverlapCore._resolve_bbg_from_isin(isin) if isin else None
            progress.progress(i / len(mnemos))

        tickers_no_equity = [t for t in resolved.values() if t]
        if not tickers_no_equity:
            st.warning("⚠️ Aucun ticker Bloomberg résolu.")
            return {m: float("nan") for m in mnemos}

        # 2) MX_Name / Currency
        st.write(f"📡 Requête Bloomberg MX_Name ({len(tickers_no_equity)} tickers)…")
        bbg_equity = [OverlapCore._as_equity(t) for t in tickers_no_equity]

        mx_names = loadedbasketbulk("BBG", bbg_equity, "MX_Name")
        _ = loadedbasketbulk("BBG", bbg_equity, "Currency")

        # 3) NMP
        st.write("🧩 Construction des identifiants NMP…")
        sec_to_nmp: Dict[str, str] = {}
        for sec, mx in mx_names.items():
            if mx:
                sec_to_nmp[sec] = "NMP." + str(mx).replace(" ", "_")

        if not sec_to_nmp:
            st.warning("⚠️ Aucun NMP construit.")
            return {m: float("nan") for m in mnemos}

        # 4) get_pose_price
        st.write("💰 Récupération des POSE (STLVAL3.16)…")
        pose_price = asyncio.run(get_pose_price(list(sec_to_nmp.values())))
        pose_dict = pose_price[0] if pose_price else {}

        # 5) map back
        out: Dict[str, float] = {}
        for m, t_no_eq in resolved.items():
            if not t_no_eq:
                out[m] = float("nan")
                continue
            sec = OverlapCore._as_equity(t_no_eq)
            nmp = sec_to_nmp.get(sec)
            out[m] = pose_dict.get(nmp, float("nan")) if nmp else float("nan")

        st.success("✅ POSE récupérées")
        return out

    # ---------------------------------------------------------------------
    # Overlap
    # ---------------------------------------------------------------------
    def getOverlapFinal(self) -> pd.DataFrame:
        st.write("📊 Calcul de l’overlap…")

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

        final_overlap = (
            pd.DataFrame.from_dict(overlap_dict, orient="index", columns=[self.etf_1])
            .sort_values(by=self.etf_1, ascending=False)
        )

        # POSE
        pose_map = self._get_pose_for_mnemos(final_overlap.index.tolist())
        final_overlap["POSE"] = final_overlap.index.map(lambda x: pose_map.get(x, float("nan")))

        st.success("🎉 Overlap + POSE calculés")
        return final_overlap
