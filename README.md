# Test


import asyncio
from io import BytesIO

import pandas as pd
import streamlit as st
import aiohttp


# ========================= ICOM CONFIG ========================= #

ICOM_BASE_URL = "https://api.icom.example.com/quotes"   # <-- adapt
ICOM_API_KEY = "YOUR_ICOM_TOKEN_HERE"                   # <-- adapt
ICOM_SIDE_FIELD = "side"                                # e.g. "BID" / "MID"
ICOM_ID_FIELD = "isin"                                  # id field returned by ICOM


# ========================= ASYNC ICOM FUNCTIONS ========================= #

async def fetch_one_fund(session: aiohttp.ClientSession, fund_id: str) -> dict:
    """
    Call ICOM API for a single fund and return a dict like:
      {fund_id: "...", side: "BID" / "MID" / ...}
    """

    params = {
        "id": fund_id,  # adapt according to ICOM spec
    }

    headers = {
        "Authorization": f"Bearer {ICOM_API_KEY}",
        "Accept": "application/json",
    }

    async with session.get(ICOM_BASE_URL, params=params, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()

        # TODO: adapt to the real ICOM response structure
        # Example: data looks like {"isin": "...", "side": "BID"}
        result = {
            "fund_id": data[ICOM_ID_FIELD],
            "side": data[ICOM_SIDE_FIELD],
        }
        return result


async def fetch_all_funds(fund_ids: list[str]) -> pd.DataFrame:
    """
    Call ICOM API in parallel for a list of fund identifiers.
    Returns a DataFrame with columns: fund_id, side
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_one_fund(session, f_id) for f_id in fund_ids]
        results = await asyncio.gather(*tasks)

    return pd.DataFrame(results)


def query_icom_sync(fund_ids: list[str]) -> pd.DataFrame:
    """
    Synchronous wrapper so we can use the asyncio logic inside Streamlit.
    """
    if not fund_ids:
        return pd.DataFrame(columns=["fund_id", "side"])

    return asyncio.run(fetch_all_funds(fund_ids))


# ========================= STREAMLIT APP ========================= #

st.set_page_config(page_title="ICOM Bid/Mid Filter", layout="wide")
st.title("Filter funds that trade at **bid** (ICOM)")

st.markdown(
    """
    1. Upload your file (CSV / XLSX)  
    2. Select the column that contains the fund identifier (ISIN, code, …)  
    3. Click **Run ICOM request**  
    4. Download the Excel file with only the funds trading at **bid**
    """
)

uploaded_file = st.file_uploader(
    "Upload a file",
    type=["xlsx", "xls", "csv"],
    help="File with a column containing fund identifiers (ISIN, etc.)"
)

df_input = None

if uploaded_file is not None:
    file_name = uploaded_file.name.lower()

    # Read the file into a DataFrame
    try:
        if file_name.endswith((".xlsx", ".xls")):
            df_input = pd.read_excel(uploaded_file)
        else:
            df_input = pd.read_csv(uploaded_file)

        st.subheader("Preview of uploaded file")
        st.dataframe(df_input.head())

    except Exception as e:
        st.error(f"Error while reading the file: {e}")


if df_input is not None:
    # Choose the column that contains the fund identifier
    id_column = st.selectbox(
        "Column with the fund identifier (ISIN / ICOM code ...)",
        options=list(df_input.columns),
    )

    # Button to call ICOM
    if st.button("Run ICOM request"):
        with st.spinner("Calling ICOM API..."):
            # List of unique identifiers
            fund_ids = df_input[id_column].dropna().astype(str).unique().tolist()

            # ICOM call (async under the hood)
            try:
                icom_df = query_icom_sync(fund_ids)
            except Exception as e:
                st.error(f"Error while calling ICOM: {e}")
                st.stop()

        if icom_df.empty:
            st.warning("ICOM returned no data.")
        else:
            st.subheader("Raw ICOM result")
            st.dataframe(icom_df)

            # Merge with original file
            merged = df_input.merge(
                icom_df,
                how="left",
                left_on=id_column,
                right_on="fund_id",
            )

            # Filter: only funds trading at BID
            bid_only = merged[merged["side"] == "BID"].copy()

            st.subheader("Filtered funds (BID quotes)")
            st.write(f"Number of rows: **{len(bid_only)}**")
            st.dataframe(bid_only)

            # Export to Excel (in memory)
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                bid_only.to_excel(writer, sheet_name="BID_ONLY", index=False)
            output.seek(0)

            st.download_button(
                label="📥 Download Excel (BID only)",
                data=output,
                file_name="funds_bid_icom.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )
