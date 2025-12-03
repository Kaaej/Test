# helpers/nav_valuation_helper.py

import asyncio
from typing import List, Dict

from .fbi_helper import ftassetcodebulk2


def get_nav_valuation_bulk(
    having_type: str,
    identifiers: List[str],
    target_type: str = "NAV_VALUATION",
) -> Dict[str, str]:
    """
    Synchronous wrapper around fbi_helper.ftassetcodebulk2.

    Parameters
    ----------
    having_type : str
        Source attribute name in FBI (e.g. 'listingid', 'ric', 'isin', 'ftassetcode', ...)
    identifiers : list[str]
        List of identifiers to query.
    target_type : str
        Target attribute name in FBI, here 'NAV_VALUATION' by default.

    Returns
    -------
    dict[str, str]
        Mapping {identifier -> NAV_VALUATION string}
    """

    if not identifiers:
        return {}

    async def _run():
        # ftassetcodebulk2 is async in your fbi_helper
        return await ftassetcodebulk2(having_type, identifiers, target_type)

    # In a classic script / Streamlit app there is no running loop, so asyncio.run is fine.
    return asyncio.run(_run())
