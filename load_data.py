import pandas as pd
import numpy as np
import re
import sqlite3
from pathlib import Path

# =========================
# Paths
# =========================
XLSX = "/mnt/data/SPICES DATABASE.xlsx"
DB   = "/mnt/data/spices.db"

TRUE_SET = {"yes", "y", "1", "true", "-"}

# =========================
# Helpers
# =========================

def norm_col(c: str) -> str:
    c = "" if c is None else str(c)
    c = re.sub(r"\s+", " ", c).strip().lower()
    return c

def clean_name(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    return x if x else None

def to_bool(x) -> int:
    if pd.isna(x):
        return 0
    s = str(x).strip().lower()
    return 1 if s in TRUE_SET else 0

def parse_cid(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s.lower() in {"na", "nil", "-", ""}:
        return None
    try:
        return int(float(s))
    except Exception:
        return None

# =========================
# Column mapping (clean)
# =========================

COLMAP = {
    # spice name variants
    "spices": "spice_name",
    "spice": "spice_name",
    "spices name": "spice_name",
    "spice name": "spice_name",

    # botanical
    "botanical": "botanical_name",
    "botanical name": "botanical_name",
    "botanical name(s)": "botanical_name",
    "botanacial": "botanical_name",
    "botanacial name": "botanical_name",

    # phytochemical
    "phytochemical": "phyto_name",
    "phytochemical name": "phyto_name",
    "phytochemical names": "phyto_name",
    "phytochemicals name": "phyto_name",
    "phyto chemical": "phyto_name",
    "phyto chemical name": "phyto_name",
    "phyto chemicals": "phyto_name",
    "phyto chemicals name": "phyto_name",
    "phtochemicals name": "phyto_name",
    "phtochemical name": "phyto_name",
    "phtochemical names": "phyto_name",
    "pytochemical": "phyto_name",

    # cid
    "cid": "cid",

    # 2D/3D
    "2d": "has_2d",
    "3d": "has_3d",
    "2d conformer": "has_2d",
    "3d conformer": "has_3d",
}

REQUIRED_COLS = ["spice_name", "botanical_name", "phyto_name", "cid", "has_2d", "has_3d"]

def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    # remove "Unnamed" columns
    keep_cols = [c for c in df.columns if not norm_col(c).startswith("unnamed")]
    df = df.loc[:, keep_cols]

    new_cols = []
    for c in df.columns:
        key = norm_col(c)

        # handle hidden unicode/whitespace junk robustly
        key = (
            key.replace("\ufeff", "")
               .replace("\u200b", "")
               .replace("\xa0", " ")
        )
        key = re.sub(r"\s+", " ", key).strip()

        mapped = COLMAP.get(key, key)
        new_cols.append(mapped)

    df.columns = new_cols

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}. Available: {list(df.columns)}")

    return df[REQUIRED_COLS].copy()

# =========================
# Schema
# =========================

def ensure_schema(con: sqlite3.Connection):
    con.execute("PRAGMA foreign_keys=ON")

    # spices
    con.execute("""
    CREATE TABLE IF NOT EXISTS spices (
        spice_id INTEGER PRIMARY KEY AUTOINCREMENT,
        spice_name TEXT UNIQUE NOT NULL,
        botanical_name TEXT
    )
    """)

    # phytochemicals
    con.execute("""
    CREATE TABLE IF NOT EXISTS phytochemicals (
        phyto_id INTEGER PRIMARY KEY AUTOINCREMENT,
        phyto_name TEXT NOT NULL,
        cid INTEGER UNIQUE
    )
    """)

    # mapping table
    con.execute("""
    CREATE TABLE IF NOT EXISTS spice_phytochemicals (
        spice_id INTEGER NOT NULL,
        phyto_id INTEGER NOT NULL,
        PRIMARY KEY (spice_id, phyto_id),
        FOREIGN KEY (spice_id) REFERENCES spices(spice_id) ON DELETE CASCADE,
        FOREIGN KEY (phyto_id) REFERENCES phytochemicals(phyto_id) ON DELETE CASCADE
    )
    """)

    # structures
    con.execute("""
    CREATE TABLE IF NOT EXISTS structures (
        phyto_id INTEGER PRIMARY KEY,
        has_2d INTEGER DEFAULT 0,
        has_3d INTEGER DEFAULT 0,
        sdf_2d_path TEXT,
        sdf_3d_path TEXT,
        png_2d_path TEXT,
        FOREIGN KEY (phyto_id) REFERENCES phytochemicals(phyto_id) ON DELETE CASCADE
    )
    """)

    # descriptors table schema matching your app.py
    con.execute("""
    CREATE TABLE IF NOT EXISTS descriptors (
        phyto_id INTEGER PRIMARY KEY,

        molecular_formula TEXT,
        molecular_weight REAL,
        xlogp REAL,
        tpsa REAL,
        hbd INTEGER,
        hba INTEGER,
        rotatable_bonds INTEGER,
        heavy_atom_count INTEGER,
        complexity REAL,
        charge INTEGER,

        smiles TEXT,
        isomeric_smiles TEXT,
        inchi TEXT,
        inchikey TEXT,
        iupac_name TEXT,

        FOREIGN KEY (phyto_id) REFERENCES phytochemicals(phyto_id) ON DELETE CASCADE
    )
    """)

    con.commit()

# =========================
# Load data
# =========================

def load():
    xlsx_path = Path(XLSX)
    db_path = Path(DB)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel not found: {xlsx_path}")
    if not db_path.exists():
        # will create new db
        db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    ensure_schema(con)

    xl = pd.ExcelFile(str(xlsx_path))
    print("Sheets:", xl.sheet_names)

    # prepared SQL (fast, safe)
    sql_ins_spice = "INSERT OR IGNORE INTO spices(spice_name, botanical_name) VALUES (?, ?)"
    sql_get_spice_id = "SELECT spice_id FROM spices WHERE spice_name = ?"

    sql_ins_phyto_cid = "INSERT OR IGNORE INTO phytochemicals(phyto_name, cid) VALUES (?, ?)"
    sql_get_phyto_by_cid = "SELECT phyto_id, phyto_name FROM phytochemicals WHERE cid = ?"

    sql_ins_phyto_null = "INSERT INTO phytochemicals(phyto_name, cid) VALUES (?, NULL)"
    sql_last_id = "SELECT last_insert_rowid()"

    sql_map = "INSERT OR IGNORE INTO spice_phytochemicals(spice_id, phyto_id) VALUES (?, ?)"

    sql_ins_struct = "INSERT OR IGNORE INTO structures(phyto_id, has_2d, has_3d) VALUES (?, ?, ?)"
    sql_upd_struct = """
        UPDATE structures
        SET has_2d = MAX(has_2d, ?),
            has_3d = MAX(has_3d, ?)
        WHERE phyto_id = ?
    """

    total_rows = 0
    inserted_links = 0

    for sheet in xl.sheet_names:
        raw = pd.read_excel(str(xlsx_path), sheet_name=sheet)
        df = unify_columns(raw)

        df["spice_name"] = df["spice_name"].map(clean_name)
        df["botanical_name"] = df["botanical_name"].map(clean_name)
        df["phyto_name"] = df["phyto_name"].map(clean_name)
        df["has_2d"] = df["has_2d"].map(to_bool)
        df["has_3d"] = df["has_3d"].map(to_bool)
        df["cid"] = df["cid"].map(parse_cid)

        # drop invalid
        df = df.dropna(subset=["spice_name", "phyto_name"])
        total_rows += len(df)

        for r in df.itertuples(index=False):
            spice = r.spice_name
            bot = r.botanical_name
            phyto = r.phyto_name
            cid = r.cid
            has_2d = int(r.has_2d)
            has_3d = int(r.has_3d)

            # spices
            con.execute(sql_ins_spice, (spice, bot))
            spice_id = con.execute(sql_get_spice_id, (spice,)).fetchone()[0]

            # phytochemicals: prefer CID-based UNIQUE
            if cid is not None:
                con.execute(sql_ins_phyto_cid, (phyto, int(cid)))

                row = con.execute(sql_get_phyto_by_cid, (int(cid),)).fetchone()
                if row is None:
                    # very rare fallback
                    con.execute(sql_ins_phyto_cid, (phyto, int(cid)))
                    row = con.execute(sql_get_phyto_by_cid, (int(cid),)).fetchone()

                phyto_id = row[0]

                # if name missing, update it
                if row[1] is None or str(row[1]).strip() == "":
                    con.execute("UPDATE phytochemicals SET phyto_name=? WHERE phyto_id=?", (phyto, phyto_id))

            else:
                # no CID: each row becomes unique phyto entry (as per your original behavior)
                con.execute(sql_ins_phyto_null, (phyto,))
                phyto_id = con.execute(sql_last_id).fetchone()[0]

            # mapping spice <-> phytochemical
            con.execute(sql_map, (spice_id, phyto_id))
            inserted_links += 1

            # structures
            con.execute(sql_ins_struct, (phyto_id, has_2d, has_3d))
            con.execute(sql_upd_struct, (has_2d, has_3d, phyto_id))

        con.commit()

    spices = con.execute("SELECT COUNT(*) FROM spices").fetchone()[0]
    phy = con.execute("SELECT COUNT(*) FROM phytochemicals").fetchone()[0]
    links = con.execute("SELECT COUNT(*) FROM spice_phytochemicals").fetchone()[0]
    withcid = con.execute("SELECT COUNT(*) FROM phytochemicals WHERE cid IS NOT NULL").fetchone()[0]

    con.close()

    print("\n✅ LOADED")
    print("rows processed:", total_rows)
    print("spices:", spices)
    print("phytochemicals:", phy)
    print("links:", links)
    print("with CID:", withcid)


if __name__ == "__main__":
    load()
