import os
import asyncio
from typing import Dict, List, Optional

import pandas as pd
import config
import streamlit as st
from streamlit import cache

# icom_price.py (adapte si besoin)
from icom_price import loadedbasketbulk, get_pose_price, ftassetcode


class OverlapCore:
    def __init__(self, etf_1: str) -> None:
        self.etf_1 = etf_1

    # =====================================================================
    # ANCIENS DEF (inchangés) — conservés avant les nouveaux
    # =====================================================================
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
        with open(
            os.path.join(config.FOLDER_PROJECT_TEAM, "Overlap", "list_etfs.txt"),
            "r",
            encoding="utf-8",
        ) as f:
            return [x.strip() for x in f.read().splitlines() if x.strip()]

    # =====================================================================
    # NOUVEAUX DEF (ajoutés) — POSE via ISIN -> ftassetcode -> MX_Name -> NMP
    # =====================================================================
    @staticmethod
    def _strip_equity(bbg_ticker: Optional[str]) -> Optional[str]:
        """
        ftassetcode renvoie typiquement: 'EUN5 GY Equity'
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
        if not t:
            return t
        return t if t.endswith("Equity") else f"{t} Equity"

    @staticmethod
    def _load_etf_infos_min() -> pd.DataFrame:
        """
        Charge uniquement ETF_TICKER + ISIN (plus léger).
        """
        df = pd.read_csv(config.FILE_ETF_INFOS, usecols=["ETF_TICKER", "ISIN"]).dropna()
        df.columns = [c.strip() for c in df.columns]
        df["ETF_TICKER"] = df["ETF_TICKER"].astype(str).str.strip()
        df["ISIN"] = df["ISIN"].astype(str).str.strip()
        return df.drop_duplicates()

    @staticmethod
    @st.cache_data(show_spinner=False)
    def _cached_bbg_from_isin(isin: str) -> Optional[str]:
        """
        Cache Streamlit: ISIN -> BBG ticker (sans 'Equity').
        Gain énorme sur les reruns.
        """
        try:
            full = ftassetcode("ISIN", isin, "BBG")
        except Exception:
            return None
        return OverlapCore._strip_equity(full)

    @staticmethod
    def _get_pose_for_mnemos(mnemos: List[str]) -> Dict[str, float]:
        """
        1) On ne garde que les mnemo réellement présents dans overlap (mnemos input).
        2) On récupère uniquement leurs ISIN depuis FILE_ETF_INFOS (filtré).
        3) ftassetcode(ISIN) (caché) -> BBG ticker complet (sans 'Equity')
        4) loadedbasketbulk -> MX_Name (+ Currency optionnel)
        5) NMP = 'NMP.' + mx.replace(' ', '_')
        6) get_pose_price -> POSE (STLVAL3.16)
        """
        # --- unique mnemos
        mnemos_u = pd.Index(mnemos).astype(str).unique().tolist()
        if not mnemos_u:
            return {}

        # --- filtre ETF infos uniquement sur mnemos utiles
        st.write("📄 Lecture ETFINFO (ISIN) pour les tickers de l’overlap…")
        df_infos = OverlapCore._load_etf_infos_min()
        df_sub = df_infos[df_infos["ETF_TICKER"].isin(mnemos_u)]
        mnemo_to_isin = dict(zip(df_sub["ETF_TICKER"], df_sub["ISIN"]))

        # --- Résolution ISIN -> BBG ticker (caché)
        st.write("🔎 Résolution ISIN → BBG…")
        progress = st.progress(0)
        resolved: Dict[str, Optional[str]] = {}

        total = len(mnemos_u)
        for i, m in enumerate(mnemos_u, start=1):
            isin = mnemo_to_isin.get(m)
            resolved[m] = OverlapCore._cached_bbg_from_isin(isin) if isin else None
            progress.progress(i / total)

        tickers_no_equity = [t for t in resolved.values() if t]
        if not tickers_no_equity:
            st.warning("⚠️ Aucun ticker BBG résolu via ftassetcode.")
            return {m: float("nan") for m in mnemos_u}

        # --- MX_Name / Currency
        st.write(f"📡 Bloomberg loadedbasketbulk MX_Name ({len(tickers_no_equity)} tickers)…")
        bbg_equity = [OverlapCore._as_equity(t) for t in tickers_no_equity]

        mx_names = loadedbasketbulk("BBG", bbg_equity, "MX_Name")
        _ = loadedbasketbulk("BBG", bbg_equity, "Currency")  # optionnel

        # --- NMP
        st.write("🧩 Construction des NMP…")
        sec_to_nmp: Dict[str, str] = {}
        for sec in bbg_equity:
            mx = mx_names.get(sec)
            if mx:
                sec_to_nmp[sec] = "NMP." + str(mx).replace(" ", "_")

        if not sec_to_nmp:
            st.warning("⚠️ Aucun NMP construit (MX_Name manquant).")
            return {m: float("nan") for m in mnemos_u}

        # --- POSE
        st.write("💰 Récupération des POSE (get_pose_price)…")
        pose_price = asyncio.run(get_pose_price(list(sec_to_nmp.values())))
        pose_dict = pose_price[0] if pose_price and len(pose_price) > 0 else {}

        # --- Remap mnemo -> pose
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

    # =====================================================================
    # MODIFIED: adds POSE to overlap df
    # =====================================================================
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

        # NEW: add POSE column (only resolves tickers present in overlap)
        st.write("🧮 Ajout des POSE…")
        pose_map = self._get_pose_for_mnemos(final_overlap.index.tolist())
        final_overlap["POSE"] = final_overlap.index.map(lambda x: pose_map.get(x, float("nan")))

        st.success("🎉 Overlap + POSE calculés")
        return final_overlap
