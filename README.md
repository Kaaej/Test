# Test


import pandas as pd
import config
from streamlit import cache
import os

# NEW
import asyncio
from typing import Optional, Dict, List

# NEW: fonctions déjà utilisées dans icom_price.py
# adapte l'import si ton projet les expose ailleurs
from icom_price import loadedbasketbulk, get_pose_price


class OverlapCore:
    def __init__(self, etf_1) -> None:
        self.etf_1 = etf_1

    def map_index_to_etf(self, etf):
        df = pd.read_csv(config.FILE_ETF_INFOS)
        df_final = df[["Tracked index", "ETF_TICKER"]]
        df_final.set_index("Tracked index", inplace=True)
        df_final.drop_duplicates(inplace=True)
        return df_final.loc[etf].values[0]

    @cache
    def get_top_5_overlap(self):
        df_ori = pd.read_csv(config.FILE_OVERLAP_MATRIX, index_col=0)
        df_result = df_ori.loc[self.etf_1].sort_values(ascending=False)
        df_result = pd.DataFrame(df_result)
        return df_result

    @staticmethod
    def get_all_etf():
        """
        Reads file of all etf
        :return: all etf (both physical and synthetical)
        :rtype: list
        """
        with open(config.FOLDER_PROJECT_TEAM + "/Overlap/list_etfs.txt", "r") as f:
            etfs = f.read().split("\n")
        return etfs

    # ---------------------------------------------------------------------
    # NEW HELPERS
    # ---------------------------------------------------------------------
    @staticmethod
    def _as_equity_ticker(t: str) -> str:
        """Normalise vers le format attendu par loadedbasketbulk: '<ticker> Equity'."""
        t = (t or "").strip()
        if not t:
            return t
        if t.endswith("Equity"):
            return t
        return f"{t} Equity"

    @staticmethod
    def _resolve_full_bbg_ticker(prefix: str) -> Optional[str]:
        """
        Essaie de reconstruire le ticker Bloomberg complet à partir d'un préfixe (ex: 'ECRP')
        en testant des suffixes de place (ex: 'FP', 'GR', 'LN', ...).
        Retourne le ticker SANS 'Equity' (ex: 'ECRP FP') ou None si introuvable.
        """
        if prefix is None:
            return None
        prefix = prefix.strip()
        if not prefix:
            return None

        # Si tu as déjà un ticker complet du style "ECRP FP" on le garde
        if " " in prefix:
            return prefix

        # Liste à ajuster à ton univers
        common_suffixes = [
            "FP", "GR", "LN", "NA", "SW", "GY", "IM", "US", "UW", "UN",
            "JP", "HK", "KS", "AU", "CN", "IN", "SJ", "NO", "DC", "SM",
            "MI", "MC", "PA", "BR", "LS", "AS", "ST", "HE"
        ]

        # On teste suffix par suffix via loadedbasketbulk(MX_Name)
        for suf in common_suffixes:
            candidate = f"{prefix} {suf}"
            try:
                _ = loadedbasketbulk("BBG", [OverlapCore._as_equity_ticker(candidate)], "MX_Name")
                # si pas d'exception, on considère que c'est OK
                return candidate
            except Exception:
                continue

        return None

    @staticmethod
    def _get_pose_for_tickers(tickers: List[str]) -> Dict[str, float]:
        """
        Pour une liste de tickers (préfix ou complets), renvoie un dict {ticker_original: pose}
        La pose correspond au champ STLVAL3.16 (comme dans icom_price.get_pose_price()).
        """
        # 1) Resolve tickers
        resolved_map: Dict[str, Optional[str]] = {}
        for t in tickers:
            resolved_map[t] = OverlapCore._resolve_full_bbg_ticker(t)

        # 2) Build bbg tickers list pour loadedbasketbulk
        resolved_list = [rt for rt in resolved_map.values() if rt is not None]
        if not resolved_list:
            return {t: float("nan") for t in tickers}

        bbg_equity = [OverlapCore._as_equity_ticker(rt) for rt in resolved_list]

        # 3) MX_Name et Currency (tu dis que tu en as besoin pour construire NMP)
        #    Ici on suit ta logique icom_price.py : NMP_ + MX_Name sans espaces
        mx_names = loadedbasketbulk("BBG", bbg_equity, "MX_Name")
        _ = loadedbasketbulk("BBG", bbg_equity, "Currency")  # pas indispensable pour POSE, mais tu l'as demandé

        # mx_names est supposé être un dict { "<ticker> Equity": "<mx_name>" }
        id_nmp: Dict[str, str] = {}
        for eq in bbg_equity:
            mx = mx_names.get(eq)
            if mx:
                id_nmp[eq] = "NMP_" + mx.replace(" ", "")

        if not id_nmp:
            return {t: float("nan") for t in tickers}

        # 4) Call get_pose_price (async) comme dans icom_price.py
        pose_price = asyncio.run(get_pose_price(list(id_nmp.values())))
        # pose_price = [dict_pose, dict_price] attendu (cf screenshot icom_price.py)
        dict_pose = pose_price[0] if pose_price and len(pose_price) > 0 else {}

        # 5) Remap vers tickers d'origine
        pose_by_original: Dict[str, float] = {}
        for original, resolved in resolved_map.items():
            if resolved is None:
                pose_by_original[original] = float("nan")
                continue

            eq = OverlapCore._as_equity_ticker(resolved)
            nmp = id_nmp.get(eq)
            pose_by_original[original] = dict_pose.get(nmp, float("nan"))

        return pose_by_original

    # ---------------------------------------------------------------------
    # MODIFIED: add POSE column
    # ---------------------------------------------------------------------
    def getOverlapFinal(self):
        compo_matrix = pd.read_csv(config.FILE_COMPO_MATRIX)

        compo = pd.read_csv(
            os.path.join(config.FOLDER_ICOM_PRICING, f"{self.etf_1}.csv"),
            usecols=["Instrument", "ProductClass"],
        )
        bonds = compo[compo["ProductClass"] == "Bond"]
        compo_matrix_filtered = compo_matrix.loc[
            compo_matrix["Instrument"].isin(bonds["Instrument"])
        ]

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

        # NEW: add POSE column for each ETF in the index
        pose_map = self._get_pose_for_tickers(final_overlap.index.tolist())
        final_overlap["POSE"] = final_overlap.index.map(lambda x: pose_map.get(x, float("nan")))

        return final_overlap
