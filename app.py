# app.py

import pandas as pd
import streamlit as st
from io import BytesIO

from helpers.nav_valuation_helper import get_nav_valuation_bulk


# ==========================
# CONFIG À ADAPTER
# ==========================

# Colonne d'identifiant dans ton fichier d’entrée
DEFAULT_ID_COL = "BBG Ticker"     # ex: "BBG Ticker", "RIC", "ISIN"...

# Nom de la colonne Asset class dans ton fichier
DEFAULT_ASSET_CLASS_COL = "Asset class"

# Type de l’identifiant côté FBI (sourceAttributeName dans l’URL)
FBI_HAVING_TYPE = "listingid"     # ex: "listingid", "ric", "isin", "ftassetcode", ...

# Attribut cible côté FBI (targetAttributeName)
FBI_TARGET_TYPE = "NAV_VALUATION"  # champ qui renvoie "Bid"/"Mid"/...


# ==========================
# APP STREAMLIT
# ==========================

st.set_page_config(page_title="FBI NAV Valuation Filter", layout="wide")
st.title("Filter Fixed Income funds by *NAV_VALUATION* using FBI")

st.markdown(
    """
    **Workflow**

    1. Upload a file (CSV / Excel) containing your funds  
    2. Select the *Asset class* column and the identifier column  
    3. The app keeps only rows with `Asset class = "Fixed income"`  
    4. It calls **FBI / ftassetcodebulk2** to retrieve **NAV_VALUATION**  
    5. It produces an Excel file with:
        - all fixed income + NAV_VALUATION  
        - only funds with NAV_VALUATION = `Bid`  
        - funds where NAV_VALUATION is missing (N/A)
    """
)

uploaded_file = st.file_uploader("Upload your file", type=["xlsx", "xls", "csv"])

df_input = None

# ---------- READ INPUT FILE ---------- #
if uploaded_file is not None:
    name = uploaded_file.name.lower()

    try:
        if name.endswith((".xlsx", ".xls")):
            df_input = pd.read_excel(uploaded_file)
        else:
            df_input = pd.read_csv(uploaded_file)

        st.subheader("Preview of uploaded file")
        st.dataframe(df_input.head())

    except Exception as e:
        st.error(f"Error reading file: {e}")


if df_input is not None:
    cols = df_input.columns.tolist()

    asset_col = st.selectbox(
        "Column with Asset class",
        options=cols,
        index=cols.index(DEFAULT_ASSET_CLASS_COL) if DEFAULT_ASSET_CLASS_COL in cols else 0,
    )

    id_col = st.selectbox(
        "Identifier column (used to query FBI)",
        options=cols,
        index=cols.index(DEFAULT_ID_COL) if DEFAULT_ID_COL in cols else 0,
    )

    st.write(f"FBI having_type: `{FBI_HAVING_TYPE}`, target_type: `{FBI_TARGET_TYPE}`")

    if st.button("Run FBI NAV_VALUATION filter"):
        # ---------- FILTER FIXED INCOME ---------- #
        asset_series = df_input[asset_col].astype(str)
        df_fixed = df_input[asset_series.str.upper() == "FIXED INCOME"].copy()

        if df_fixed.empty:
            st.warning('No rows with Asset class "Fixed income" found.')
        else:
            st.write(f"Rows with Asset class = Fixed income: **{len(df_fixed)}**")

            # ---------- PREPARE IDENTIFIERS ---------- #
            identifiers = (
                df_fixed[id_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            if not identifiers:
                st.error("No identifiers found in the selected identifier column.")
                st.stop()

            # ---------- CALL FBI via ftassetcodebulk2 ---------- #
            with st.spinner("Calling FBI (ftassetcodebulk2)..."):
                try:
                    nav_map = get_nav_valuation_bulk(
                        having_type=FBI_HAVING_TYPE,
                        identifiers=identifiers,
                        target_type=FBI_TARGET_TYPE,
                    )
                except Exception as e:
                    st.error(f"Error while calling FBI: {e}")
                    st.stop()

            # ---------- MAP NAV_VALUATION BACK TO ROWS ---------- #
            df_fixed["NAV_VALUATION"] = (
                df_fixed[id_col]
                .astype(str)
                .map(nav_map)       # nav_map[identifier] -> NAV_VALUATION
            )

            # Show enriched fixed-income data
            st.subheader("Fixed income funds enriched with NAV_VALUATION")
            st.dataframe(df_fixed)

            # ---------- BUILD FILTERS (BID / N/A) ---------- #
            nav_upper = df_fixed["NAV_VALUATION"].astype(str).str.strip().str.upper()

            df_bid = df_fixed[nav_upper == "BID"].copy()

            df_na = df_fixed[
                df_fixed["NAV_VALUATION"].isna()
                | (nav_upper == "")
                | (nav_upper == "NAN")
            ].copy()

            st.subheader("Funds with NAV_VALUATION = 'Bid'")
            st.write(f"Count: **{len(df_bid)}**")
            st.dataframe(df_bid)

            st.subheader("Funds with NAV_VALUATION N/A (missing / empty)")
            st.write(f"Count: **{len(df_na)}**")
            st.dataframe(df_na)

            # ---------- EXPORT TO EXCEL ---------- #
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df_input.to_excel(writer, sheet_name="INPUT_RAW", index=False)
                df_fixed.to_excel(writer, sheet_name="FIXED_INCOME_ALL", index=False)
                df_bid.to_excel(writer, sheet_name="FIXED_INCOME_BID", index=False)
                df_na.to_excel(writer, sheet_name="FIXED_INCOME_NAV_NA", index=False)
            output.seek(0)

            st.download_button(
                label="📥 Download Excel (Fixed income + NAV_VALUATION)",
                data=output,
                file_name="fixed_income_nav_valuation.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
