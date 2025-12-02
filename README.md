# Test


import pandas as pd
import streamlit as st
from io import BytesIO
import pdblp

# ---------- Bloomberg connection ----------
@st.cache_resource
def get_bbg_connection():
    con = pdblp.BCon(port=8194, timeout=5000)
    con.start()
    return con

def get_nav_valuation(con, tickers: list[str]) -> pd.DataFrame:
    bbg_tickers = [t.upper() + " Equity" for t in tickers]

    df = con.bdp(
        securities=bbg_tickers,
        fields=["NAVValuation"],    # or "NAV_VALUATION"
    )

    df = df.reset_index().rename(columns={
        "ticker": "bbg_ticker",
        "NAVValuation": "NAVValuation",
    })

    # Strip " Equity" to match original tickers
    df["ticker"] = df["bbg_ticker"].str.replace(" Equity", "", regex=False)
    return df[["ticker", "NAVValuation"]]


# ---------- Streamlit app ----------
st.set_page_config(page_title="Bloomberg Bid/Mid Filter", layout="wide")
st.title("Filter funds that trade at **Bid** using Bloomberg API")

uploaded_file = st.file_uploader(
    "Upload a file (CSV / Excel)",
    type=["csv", "xlsx", "xls"],
)

df_input = None
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
    ticker_col = st.selectbox(
        "Column with Bloomberg tickers (e.g. EUNS GY, IEAC LN)",
        options=list(df_input.columns),
    )

    if st.button("Run Bloomberg request"):
        with st.spinner("Querying Bloomberg... Bloomberg Terminal must be open"):
            con = get_bbg_connection()

            tickers = (
                df_input[ticker_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            nav_df = get_nav_valuation(con, tickers)

        st.subheader("Raw NAVValuation from Bloomberg")
        st.dataframe(nav_df)

        # Merge back to original file
        merged = df_input.merge(
            nav_df,
            how="left",
            left_on=ticker_col,
            right_on="ticker",
        )

        # Keep only Bid
        bid_only = merged[
            merged["NAVValuation"].str.upper() == "BID"
        ].copy()

        st.subheader("Funds trading at Bid")
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
            file_name="funds_bid_bloomberg.xlsx",
            mime=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
        )
