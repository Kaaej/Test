import asyncio
from pathlib import Path

import pandas as pd

# à adapter au bon chemin / package
from helpers.fbi_helper import ftassetcodebulk, query_fbi


# =====================
# CONFIG
# =====================

# colonne dans ton fichier d'entrée qui permet d'identifier les fonds
INPUT_ID_COL = "Identifier"        # TODO: "ISIN", "BBG Ticker", "RIC", etc.

# type de ce champ côté FBI (source de ftAssetCode)
HAVING_TYPE = "ric"                # TODO: "ric", "isin", "instrumentid", etc.
TARGET_TYPE = "ftassetcode"        # cible : ftassetcode


# =====================
# FBI HELPERS
# =====================

def get_ftassetcodes(identifiers: list[str]) -> dict[str, str]:
    """
    Use ftassetcodebulk from fbi_helper to translate identifiers -> ftAssetCode.

    Returns a dict {identifier: ftAssetCode}
    """
    # ftassetcodebulk is async in your helper -> run it with asyncio
    mapping = asyncio.run(ftassetcodebulk(HAVING_TYPE, identifiers, TARGET_TYPE))
    # on suppose que le helper renvoie déjà un dict {identifier: ftAssetCode}
    return mapping


def get_fbi_nav_data(ftassetcodes: list[str]) -> pd.DataFrame:
    """
    Query FBI for Asset class & NAV_Valuation for a list of ftAssetCode.

    Returns a DataFrame with columns:
      - FTASSETCODE (ou ftassetcode)
      - Asset class
      - NAV_Valuation
    """

    if not ftassetcodes:
        return pd.DataFrame(columns=["ftassetcode", "Asset class", "NAV_Valuation"])

    # On construit une clause IN propre: 'code1','code2',...
    in_list = ",".join(f"'{c}'" for c in ftassetcodes)

    # TODO: adapter le nom de la table/les champs EXACTS côté FBI.
    # Je mets un exemple typique :
    sql = f"""
        SELECT
            FTASSETCODE          AS ftassetcode,
            ASSET_CLASS          AS "Asset class",
            NAV_VALUATION        AS "NAV_Valuation"
        FROM FBI_FUND_NAV        -- TODO: remplace par ta vraie table FBI
        WHERE FTASSETCODE IN ({in_list})
    """

    rows, field_names = query_fbi(sql)   # ton helper retourne (rows, field_names)
    df = pd.DataFrame(rows, columns=field_names)
    return df


# =====================
# MAIN LOGIC
# =====================

def process_file(input_path: str | Path, output_path: str | Path):
    input_path = Path(input_path)
    output_path = Path(output_path)

    # --- 1. Lire le fichier d’entrée --- #
    if input_path.suffix.lower() in [".xlsx", ".xls"]:
        df_in = pd.read_excel(input_path)
    else:
        df_in = pd.read_csv(input_path)

    print("Input preview:")
    print(df_in.head())

    if INPUT_ID_COL not in df_in.columns:
        raise ValueError(f"Column '{INPUT_ID_COL}' not found in input file")

    identifiers = (
        df_in[INPUT_ID_COL]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    print(f"Number of unique identifiers: {len(identifiers)}")

    # --- 2. Identifiers -> ftAssetCode via FBI helper --- #
    id_to_ft = get_ftassetcodes(identifiers)

    # map dans le dataframe
    df_in["ftassetcode"] = df_in[INPUT_ID_COL].astype(str).map(id_to_ft)

    # --- 3. Récupérer Asset class & NAV_Valuation depuis FBI --- #
    ft_list = (
        df_in["ftassetcode"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    df_fbi = get_fbi_nav_data(ft_list)

    print("FBI data preview:")
    print(df_fbi.head())

    # --- 4. Merge input + infos FBI --- #
    df_merged = df_in.merge(
        df_fbi,
        how="left",
        on="ftassetcode"
    )

    # --- 5. Filtrer Asset class = Fixed income --- #
    df_fixed = df_merged[df_merged["Asset class"].str.upper() == "FIXED INCOME"]

    # --- 6. Séparer BID & N/A --- #
    nav_val = df_fixed["NAV_Valuation"].astype(str).str.upper()

    df_bid = df_fixed[nav_val == "BID"].copy()
    # N/A = valeurs manquantes ou chaîne vide
    df_na = df_fixed[df_fixed["NAV_Valuation"].isna() | (nav_val == "NAN") | (nav_val == "")].copy()

    print(f"Fixed income total: {len(df_fixed)}")
    print(f"  - BID: {len(df_bid)}")
    print(f"  - NAV_Valuation N/A: {len(df_na)}")

    # --- 7. Sauvegarde Excel avec plusieurs onglets --- #
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        df_merged.to_excel(writer, sheet_name="ALL_WITH_FBI", index=False)
        df_bid.to_excel(writer, sheet_name="FIXED_INCOME_BID", index=False)
        df_na.to_excel(writer, sheet_name="FIXED_INCOME_NAV_NA", index=False)

    print(f"Written output to: {output_path}")


if __name__ == "__main__":
    # Exemple : adapte les chemins comme tu veux
    process_file(
        input_path=r"input_funds.xlsx",         # fichier source (du "folder")
        output_path=r"output_funds_fbi.xlsx",   # fichier de sortie
    )
