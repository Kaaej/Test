# Test


import pandas as pd
import streamlit as st
from io import BytesIO

from helpers.icom_nav_side import get_sides_sync

st.set_page_config(page_title="ICOM Bid Filter (ftAssetCode)", layout="wide")
st.title("Filter funds that quote at **Bid** using ICOM (ftAssetCode)")

st.markdown(
    """
    1. Upload a file (CSV / Excel)  
    2. Choose the column containing **ftAssetCode**  
    3. Click **Run ICOM request**  
    4. Download the Excel file with only **Bid** funds
    """
)

uploaded_file = st.file_uploader(
    "Upload a file",
    type=["csv", "xlsx", "xls"],
)

df_input = None

# ----- Read file ----- #
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
        st.error(f"Error while reading the file: {e}")


if df_input is not None:
    ft_col = st.selectbox(
        "Column with ftAssetCode",
        options=list(df_input.columns),
    )

    if st.button("Run ICOM request"):
        # liste d’asset codes uniques
        ft_codes = (
            df_input[ft_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
        )

        with st.spinner("Querying ICOM asynchronously..."):
            try:
                icom_df = get_sides_sync(ft_codes)
            except Exception as e:
                st.error(f"Error while calling ICOM: {e}")
                st.stop()

        if icom_df.empty:
            st.warning("ICOM returned no data.")
        else:
            st.subheader("Raw ICOM result (asset_code / side)")
            st.dataframe(icom_df)

            merged = df_input.merge(
                icom_df,
                how="left",
                left_on=ft_col,
                right_on="asset_code",
            )

            merged["side_upper"] = merged["side"].astype(str).str.upper()
            bid_only = merged[merged["side_upper"] == "BID"].copy()

            st.subheader("Funds quoting at **Bid**")
            st.write(f"Rows: **{len(bid_only)}**")
            st.dataframe(bid_only)

            # Export Excel
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                bid_only.to_excel(writer, sheet_name="BID_ONLY", index=False)
            output.seek(0)

            st.download_button(
                label="📥 Download Excel (Bid only)",
                data=output,
                file_name="funds_bid_icom.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )




NAV SIDE : 
# helpers/icom_nav_side.py
import asyncio
from typing import List, Dict
import pandas as pd

from .icom_session import create_session


# -------------------------
#  RÉCUPÉRATION D’UN SEUL CODE
# -------------------------

async def fetch_one_side_from_sql(ft_asset_code: str) -> Dict[str, str]:
    session = await create_session()

    # TODO — ADAPTER ICI : table, colonne ftAssetCode, colonne du side (BID/MID)
    sql = f"""
        SELECT
            FTASSET_CODE        AS asset_code,
            NAV_VALUATION_SIDE  AS side
        FROM FUND_NAV
        WHERE FTASSET_CODE = '{ft_asset_code}'
    """

    rows = await session.sqlstatement(sql)

    # Conversion générique : rows est un DataList → on le transforme en dict
    rows_list = [row for row in rows]

    if not rows_list:
        return {"asset_code": ft_asset_code, "side": None}

    row0 = rows_list[0]

    return {
        "asset_code": row0.get("asset_code", ft_asset_code),
        "side": row0.get("side"),
    }


# -------------------------
#  RÉCUPÉRATION PAR LOTS
# -------------------------

async def fetch_all_sides_from_sql(ft_asset_codes: List[str]) -> pd.DataFrame:
    tasks = [fetch_one_side_from_sql(code) for code in ft_asset_codes]
    results = await asyncio.gather(*tasks)
    return pd.DataFrame(results)


def get_sides_sync(ft_asset_codes: List[str]) -> pd.DataFrame:
    """
    Wrapper synchronisé pour Streamlit.
    Streamlit ne gère pas l'async → on utilise asyncio.run.
    """
    if not ft_asset_codes:
        return pd.DataFrame(columns=["asset_code", "side"])

    return asyncio.run(fetch_all_sides_from_sql(ft_asset_codes))

