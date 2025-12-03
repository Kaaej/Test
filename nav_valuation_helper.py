# helpers/nav_valuation_helper.py

# helpers/nav_valuation_helper.py

import asyncio
from typing import List, Dict

from .fbi_helper import ftassetcodebulk2


def get_nav_valuation_bulk_bbg(identifiers: List[str]) -> Dict[str, str]:
    """
    Synchronous wrapper around ftassetcodebulk2 for Bloomberg tickers.

    Parameters
    ----------
    identifiers : list[str]
        List of Bloomberg tickers already formatted like: "IEAC LN Equity".

    Returns
    -------
    dict[str, str]
        Mapping such as:
        {
            "IEAC LN Equity": "Bid",
            "XYZ LN Equity": "Mid",
            ...
        }
        Missing or unknown tickers return None.
    """

    if not identifiers:
        return {}

    async def _run():
        # Call FBI using the same convention as your Excel ftAssetCode:
        # sourceAttributeName = "BBG"
        # targetAttributeName = "NAVValuation"
        return await ftassetcodebulk2("BBG", identifiers, "NAVValuation")

    # Streamlit runs in a non-async context → asyncio.run() is appropriate
    return asyncio.run(_run())
