# app.py

# app.py

import pandas as pd
import streamlit as st
from io import BytesIO

from helpers.nav_valuation_helper import get_nav_valuation_bulk_bbg


# ==========================
# CONFIGURATION
# ==========================

DEFAULT_BBG_COL = "BBG Ticker"       # Column containing Bloomberg tickers
DEFAULT_ASSET_CLASS_COL = "Asset class"


# ==========================
# STREAMLIT APPLICATION
# ==========================

st.set_page_config(page_title="Fixed Income NAVValuation Filter", layout="wide")
st.title("Filter Fixed Income Funds by NAVValuation (via Bloomberg tickers & FBI)")

st.markdown(
    """
    ### How this tool works

    1. Upload a CSV or Excel file containing your fund list  
    2. Select the *Asset class* and *Bloomberg ticker* columns  
    3. The app keeps only rows where `Asset class = "Fixed Income"`  
    4. For each Bloomberg ticker, the app generates `UPPER(ticker) + " Equity"`  
    5. It sends a bulk request:

       ```
       ftassetcodebulk2("BBG", [...], "NAVValuation")
       ```

    6. It produces an Excel file containing:
       - All Fixed Income funds enriched with NAVValuation  
       - Only funds with NAVValuation = "Bid"  
       - Funds where NAVValuation is missing (N/A)  
    """
)

uploaded_file = st.file_uploader("Upload your input file", type=["xlsx", "xls", "csv"])

df_input = None

# ----------------------------------------------------------
# Step 1 — Load input file
# ----------------------------------------------------------
if uploaded_file is not None:
    filename = uploaded_file.name.lower()

    try:
        if filename.endswith((".xlsx", ".xls")):
            df_input = pd.read_excel(uploaded_file)
        else:
            df_input = pd.read_csv(uploaded_file)

        st.subheader("Preview of uploaded file")
        st.dataframe(df_input.head())

    except Exception as e:
        st.error(f"Unable to read file: {e}")


# ----------------------------------------------------------
# Step 2 — Configure columns
# ----------------------------------------------------------
if df_input is not None:
    columns = df_input.columns.tolist()

    asset_col = st.selectbox(
        "Select the Asset class column:",
        options=columns,
        index=columns.index(DEFAULT_ASSET_CLASS_COL)
        if DEFAULT_ASSET_CLASS_COL in columns else 0,
    )

    bbg_col = st.selectbox(
        "Select the Bloomberg ticker column:",
        options=columns,
        index=columns.index(DEFAULT_BBG_COL)
        if DEFAULT_BBG_COL in columns else 0,
    )

    # ----------------------------------------------------------
    # Step 3 — Run computation
    # ----------------------------------------------------------
    if st.button("Run NAVValuation Filter"):
        # ---- Filter Fixed Income ----
        df_fi = df_input[
            df_input[asset_col].astype(str).str.upper() == "FIXED INCOME"
        ].copy()

        if df_fi.empty:
            st.warning('No rows found with Asset class = "Fixed Income".')
            st.stop()

        st.write(f"Rows detected as Fixed Income: **{len(df_fi)}**")

        # ---- Build valid Bloomberg tickers for FBI ----
        bbg_list = (
            df_fi[bbg_col]
            .astype(str)
            .str.upper()
            .str.strip()
            .apply(lambda x: f"{x} Equity")
            .tolist()
        )

        # ---- Call FBI ----
        with st.spinner("Requesting NAVValuation from FBI (ftassetcodebulk2)..."):
            try:
                nav_map = get_nav_valuation_bulk_bbg(bbg_list)
            except Exception as e:
                st.error(f"Error calling FBI: {e}")
                st.stop()

        # ---- Map NAVValuation back to DataFrame ----
        df_fi["NAVValuation"] = [
            nav_map.get(ticker, None) for ticker in bbg_list
        ]

        st.subheader("Fixed Income funds enriched with NAVValuation")
        st.dataframe(df_fi)

        # ---- Build subsets ----
        nav_upper = df_fi["NAVValuation"].astype(str).str.upper()

        df_bid = df_fi[nav_upper == "BID"]
        df_na = df_fi[df_fi["NAVValuation"].isna() | (nav_upper == "") | (nav_upper == "NAN")]

        st.subheader("Funds with NAVValuation = 'Bid'")
        st.write(f"Count: **{len(df_bid)}**")
        st.dataframe(df_bid)

        st.subheader("Funds with NAVValuation missing (N/A)")
        st.write(f"Count: **{len(df_na)}**")
        st.dataframe(df_na)

        # ----------------------------------------------------------
        # Step 4 — Generate output Excel
        # ----------------------------------------------------------
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_input.to_excel(writer, sheet_name="INPUT_RAW", index=False)
            df_fi.to_excel(writer, sheet_name="FIXED_INCOME_ALL", index=False)
            df_bid.to_excel(writer, sheet_name="ONLY_BID", index=False)
            df_na.to_excel(writer, sheet_name="NAV_NA", index=False)

        output.seek(0)

        st.download_button(
            label="📥 Download Excel Output",
            data=output,
            file_name="fixed_income_navvaluation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
