import os
import sqlite3
import time
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

from flask import Flask, render_template, request, jsonify, abort

# =========================================================
# Config
# =========================================================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# ✅ Single DB in /data
DB_PATH = os.path.join(BASE_DIR, "data", "spices.db")

# ✅ Static structure folders
STRUCT_BASE = os.path.join(BASE_DIR, "static", "structures")
SDF2D_DIR = os.path.join(STRUCT_BASE, "sdf2d")
SDF3D_DIR = os.path.join(STRUCT_BASE, "sdf3d")
PNG_DIR = os.path.join(STRUCT_BASE, "png")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SpicesDB/1.0"

app = Flask(__name__)

# =========================================================
# DB Helpers
# =========================================================

def ensure_structure_dirs():
    os.makedirs(SDF2D_DIR, exist_ok=True)
    os.makedirs(SDF3D_DIR, exist_ok=True)
    os.makedirs(PNG_DIR, exist_ok=True)


def get_db_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"SQLite DB not found at {DB_PATH}. Put spices.db inside /data folder."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query_db(sql: str, params=(), one: bool = False):
    conn = get_db_connection()
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return (rows[0] if rows else None) if one else rows


def build_base_qs():
    """Build querystring without 'page' so pagination can append page=<n>."""
    args = request.args.to_dict(flat=True)
    args.pop("page", None)
    return urlencode(args)


def ensure_descriptors_table_schema():
    """
    Safety guard: ensures descriptors table exists with expected column names.
    Prevents schema mismatch issues (smiles/inchikey not inserting).
    """
    conn = get_db_connection()
    conn.execute("""
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

          FOREIGN KEY (phyto_id) REFERENCES phytochemicals(phyto_id)
        )
    """)
    conn.commit()
    conn.close()


# =========================================================
# PubChem: download helpers (ON-DEMAND)
# =========================================================

def download_file(url: str, dest: str, timeout: int = 25, retries: int = 3) -> bool:
    """
    Robust downloader with retry + backoff.
    Skips if file already exists and is non-empty.
    """
    try:
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            return True
    except OSError:
        pass

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                content = r.read()

            # PubChem sometimes returns HTML error page / invalid tiny responses
            if not content or len(content) < 100:
                raise ValueError("Downloaded content too small/invalid")

            with open(dest, "wb") as f:
                f.write(content)

            return True

        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError) as e:
            print(f"[Download] attempt {attempt}/{retries} failed: {e}")
            time.sleep(0.8 * attempt)
        except Exception as e:
            print(f"[Download] attempt {attempt}/{retries} failed: {e}")
            time.sleep(0.8 * attempt)

    return False


def upsert_structure_paths(phyto_id: int, cid: int, got2d: bool, got3d: bool, gotpng: bool):
    """
    Ensure structures row exists and update paths for downloaded files.
    Paths are relative to Flask /static.
    """
    conn = get_db_connection()

    conn.execute(
        """
        INSERT INTO structures (phyto_id, has_2d, has_3d, sdf_2d_path, sdf_3d_path, png_2d_path)
        VALUES (?, 0, 0, NULL, NULL, NULL)
        ON CONFLICT(phyto_id) DO NOTHING
        """,
        (phyto_id,),
    )

    if got2d:
        conn.execute(
            "UPDATE structures SET has_2d = 1, sdf_2d_path = ? WHERE phyto_id = ?",
            (f"structures/sdf2d/CID_{cid}.sdf", phyto_id),
        )
    if got3d:
        conn.execute(
            "UPDATE structures SET has_3d = 1, sdf_3d_path = ? WHERE phyto_id = ?",
            (f"structures/sdf3d/CID_{cid}.sdf", phyto_id),
        )
    if gotpng:
        conn.execute(
            "UPDATE structures SET png_2d_path = ? WHERE phyto_id = ?",
            (f"structures/png/CID_{cid}.png", phyto_id),
        )

    conn.commit()
    conn.close()


def fetch_structures_on_demand(phyto_id: int, cid: int) -> bool:
    """
    Download missing structure files for this CID and update DB.
    Returns True if any file succeeded.
    """
    if not cid:
        return False

    ensure_structure_dirs()

    sdf2d_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/SDF?record_type=2d"
    sdf3d_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/SDF?record_type=3d"
    png_url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/PNG"

    sdf2d_dest = os.path.join(SDF2D_DIR, f"CID_{cid}.sdf")
    sdf3d_dest = os.path.join(SDF3D_DIR, f"CID_{cid}.sdf")
    png_dest = os.path.join(PNG_DIR, f"CID_{cid}.png")

    got2d = download_file(sdf2d_url, sdf2d_dest)
    got3d = download_file(sdf3d_url, sdf3d_dest)
    gotpng = download_file(png_url, png_dest)

    if got2d or got3d or gotpng:
        upsert_structure_paths(
            phyto_id=phyto_id,
            cid=cid,
            got2d=got2d,
            got3d=got3d,
            gotpng=gotpng,
        )
        time.sleep(0.2)
        return True

    return False

# =========================================================
# PubChem: descriptors + formats fetch (ON-DEMAND)
#   ✅ includes TXT fallback logic for missing SMILES
# =========================================================

def _pubchem_get_properties(cid: int, props: list[str]) -> dict:
    """
    Request PubChem property JSON and return Properties[0] dict.
    Raises exception if not available.
    """
    url = (
        f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/property/"
        + ",".join(props)
        + "/JSON"
    )
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=25) as r:
        data = json.loads(r.read().decode("utf-8"))

    return data["PropertyTable"]["Properties"][0]


def _pubchem_get_txt(cid: int, prop: str):
    """
    Fetch a single PubChem property using TXT endpoint.
    Returns string or None.
    """
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/property/{prop}/TXT"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=25) as r:
            txt = r.read().decode("utf-8").strip()
        return txt if txt and txt.lower() != "null" else None
    except Exception:
        return None


def fetch_descriptors_from_pubchem(phyto_id: int, cid: int) -> bool:
    """
    Fetch descriptors + formats from PubChem (CID) and store into descriptors table.
    Uses TXT fallbacks for CanonicalSMILES / IsomericSMILES if missing from JSON.
    """
    if not cid:
        return False

    ensure_descriptors_table_schema()

    props = [
        "MolecularFormula",
        "MolecularWeight",
        "XLogP",
        "TPSA",
        "HBondDonorCount",
        "HBondAcceptorCount",
        "RotatableBondCount",
        "HeavyAtomCount",
        "Complexity",
        "Charge",

        # formats
        "CanonicalSMILES",
        "IsomericSMILES",
        "InChI",
        "InChIKey",
        "IUPACName",
    ]

    try:
        props_obj = _pubchem_get_properties(cid, props)

        row = {
            "molecular_formula": props_obj.get("MolecularFormula"),
            "molecular_weight": props_obj.get("MolecularWeight"),
            "xlogp": props_obj.get("XLogP"),
            "tpsa": props_obj.get("TPSA"),
            "hbd": props_obj.get("HBondDonorCount"),
            "hba": props_obj.get("HBondAcceptorCount"),
            "rotatable_bonds": props_obj.get("RotatableBondCount"),
            "heavy_atom_count": props_obj.get("HeavyAtomCount"),
            "complexity": props_obj.get("Complexity"),
            "charge": props_obj.get("Charge"),

            "smiles": props_obj.get("CanonicalSMILES"),
            "isomeric_smiles": props_obj.get("IsomericSMILES"),
            "inchi": props_obj.get("InChI"),
            "inchikey": props_obj.get("InChIKey"),
            "iupac_name": props_obj.get("IUPACName"),
        }

        # ✅ TXT fallback for SMILES (handles your exact problem cases)
        if row["smiles"] is None:
            row["smiles"] = _pubchem_get_txt(cid, "CanonicalSMILES")

        if row["isomeric_smiles"] is None:
            row["isomeric_smiles"] = _pubchem_get_txt(cid, "IsomericSMILES")

        print("\n========== DESCRIPTORS DEBUG ==========")
        print("phyto_id:", phyto_id)
        print("cid:", cid)
        print("iupac:", row["iupac_name"])
        print("smiles:", row["smiles"])
        print("isomeric_smiles:", row["isomeric_smiles"])
        print("inchikey:", row["inchikey"])
        print("inchi:", "present" if row["inchi"] else None)
        print("======================================\n")

        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO descriptors (
              phyto_id,
              molecular_formula, molecular_weight, xlogp, tpsa,
              hbd, hba, rotatable_bonds, heavy_atom_count,
              complexity, charge,
              smiles, isomeric_smiles, inchi, inchikey, iupac_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(phyto_id) DO UPDATE SET
              molecular_formula=excluded.molecular_formula,
              molecular_weight=excluded.molecular_weight,
              xlogp=excluded.xlogp,
              tpsa=excluded.tpsa,
              hbd=excluded.hbd,
              hba=excluded.hba,
              rotatable_bonds=excluded.rotatable_bonds,
              heavy_atom_count=excluded.heavy_atom_count,
              complexity=excluded.complexity,
              charge=excluded.charge,
              smiles=excluded.smiles,
              isomeric_smiles=excluded.isomeric_smiles,
              inchi=excluded.inchi,
              inchikey=excluded.inchikey,
              iupac_name=excluded.iupac_name
            """,
            (
                phyto_id,
                row["molecular_formula"], row["molecular_weight"], row["xlogp"], row["tpsa"],
                row["hbd"], row["hba"], row["rotatable_bonds"], row["heavy_atom_count"],
                row["complexity"], row["charge"],
                row["smiles"], row["isomeric_smiles"], row["inchi"], row["inchikey"], row["iupac_name"],
            ),
        )
        conn.commit()
        conn.close()

        time.sleep(0.2)
        return True

    except Exception as e:
        print(f"[Descriptors] Failed (phyto_id={phyto_id}, CID={cid}): {e}")
        return False
# =========================================================
# Drug-likeness rules
# =========================================================

def calc_druglikeness_rules(d):
    """
    d is sqlite3.Row/dict-like object containing:
    molecular_weight, xlogp, tpsa, hbd, hba, rotatable_bonds
    Returns dict with rule results.
    """

    def _num(x):
        return None if x is None else float(x)

    mw = _num(d["molecular_weight"])
    xlogp = _num(d["xlogp"])
    tpsa = _num(d["tpsa"])
    hbd = d["hbd"]
    hba = d["hba"]
    rot = d["rotatable_bonds"]

    results = {}

    # Lipinski
    fails = []
    if mw is not None and mw > 500: fails.append("MW > 500")
    if xlogp is not None and xlogp > 5: fails.append("XlogP > 5")
    if hbd is not None and hbd > 5: fails.append("HBD > 5")
    if hba is not None and hba > 10: fails.append("HBA > 10")
    results["Lipinski"] = {"pass": len(fails) == 0, "fail_count": len(fails), "fails": fails, "note": "Rule of 5"}

    # Veber
    fails = []
    if rot is not None and rot > 10: fails.append("RotB > 10")
    if tpsa is not None and tpsa > 140: fails.append("TPSA > 140")
    results["Veber"] = {"pass": len(fails) == 0, "fail_count": len(fails), "fails": fails, "note": "Oral bioavailability"}

    # Ghose (partial)
    fails = []
    if mw is not None and (mw < 160 or mw > 480): fails.append("MW not in 160–480")
    if xlogp is not None and (xlogp < -0.4 or xlogp > 5.6): fails.append("XLogP not in −0.4–5.6")
    results["Ghose"] = {"pass": len(fails) == 0, "fail_count": len(fails), "fails": fails, "note": "Drug-likeness (Ghose)"}

    # Egan
    fails = []
    if tpsa is not None and tpsa > 131.6: fails.append("TPSA > 131.6")
    if xlogp is not None and xlogp > 5.88: fails.append("XlogP > 5.88")
    results["Egan"] = {"pass": len(fails) == 0, "fail_count": len(fails), "fails": fails, "note": "Absorption/permeation"}

    # Muegge
    fails = []
    if mw is not None and (mw < 200 or mw > 600): fails.append("MW not in 200–600")
    if xlogp is not None and xlogp > 5: fails.append("XLogP > 5")
    if tpsa is not None and tpsa > 150: fails.append("TPSA > 150")
    if hbd is not None and hbd > 5: fails.append("HBD > 5")
    if hba is not None and hba > 10: fails.append("HBA > 10")
    if rot is not None and rot > 15: fails.append("RotB > 15")
    results["Muegge"] = {"pass": len(fails) == 0, "fail_count": len(fails), "fails": fails, "note": "Drug-likeness (Muegge)"}

    return results


# =========================================================
# Routes
# =========================================================

@app.route("/")
def home():
    counts = {
        "spices": query_db("SELECT COUNT(*) AS c FROM spices", one=True)["c"],
        "phytochemicals": query_db("SELECT COUNT(*) AS c FROM phytochemicals", one=True)["c"],
        "links": query_db("SELECT COUNT(*) AS c FROM spice_phytochemicals", one=True)["c"],
    }
    return render_template("home_spices.html", counts=counts)


# -------------------------
# Browse Spices
# -------------------------

@app.route("/browse/spices")
def browse_spices():
    q = (request.args.get("q") or "").strip()
    starts = (request.args.get("starts") or "").strip().upper()

    sort = request.args.get("sort", "name_asc")
    sort_map = {
        "name_asc": "spice_name ASC",
        "name_desc": "spice_name DESC",
    }
    order_by = sort_map.get(sort, "spice_name ASC")

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    per_page = max(5, min(per_page, 200))
    offset = (page - 1) * per_page

    base_qs = build_base_qs()

    if starts and len(starts) == 1 and starts.isalpha():
        total = query_db(
            "SELECT COUNT(*) AS c FROM spices WHERE spice_name LIKE ?",
            (f"{starts}%",),
            one=True,
        )["c"]

        rows = query_db(
            f"""
            SELECT spice_id, spice_name, botanical_name
            FROM spices
            WHERE spice_name LIKE ?
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            (f"{starts}%", per_page, offset),
        )

    elif q:
        total = query_db(
            "SELECT COUNT(*) AS c FROM spices WHERE spice_name LIKE ?",
            (f"%{q}%",),
            one=True,
        )["c"]

        rows = query_db(
            f"""
            SELECT spice_id, spice_name, botanical_name
            FROM spices
            WHERE spice_name LIKE ?
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            (f"%{q}%", per_page, offset),
        )

    else:
        total = query_db("SELECT COUNT(*) AS c FROM spices", one=True)["c"]

        rows = query_db(
            f"""
            SELECT spice_id, spice_name, botanical_name
            FROM spices
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            (per_page, offset),
        )

    return render_template(
        "browse_spices.html",
        spices=rows,
        q=q,
        starts=starts,
        sort=sort,
        page=page,
        per_page=per_page,
        total=total,
        base_qs=base_qs,
    )


@app.route("/api/search/spices")
def api_search_spices():
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 20)), 50)
    if not q:
        return jsonify([])

    rows = query_db(
        """
        SELECT spice_id, spice_name, botanical_name
        FROM spices
        WHERE spice_name LIKE ?
        ORDER BY spice_name ASC
        LIMIT ?
        """,
        (f"{q}%", limit),
    )

    return jsonify([
        {"spice_id": r["spice_id"], "spice_name": r["spice_name"], "botanical_name": r["botanical_name"]}
        for r in rows
    ])


# -------------------------
# Browse Phytochemicals
# -------------------------

@app.route("/browse/phytochemicals")
def browse_phytochemicals():
    q = (request.args.get("q") or "").strip()

    only_cid = request.args.get("only_cid") == "1"
    only_3d = request.args.get("only_3d") == "1"
    only_2d = request.args.get("only_2d") == "1"

    # contradictory
    if only_2d and only_3d:
        only_2d = False
        only_3d = False

    sort = request.args.get("sort", "name_asc")
    sort_map = {
        "name_asc": "p.phyto_name ASC",
        "name_desc": "p.phyto_name DESC",
        "cid_asc": "p.cid ASC",
        "cid_desc": "p.cid DESC",
        "spice_count_desc": "spice_count DESC",
    }
    order_by = sort_map.get(sort, "p.phyto_name ASC")

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    per_page = max(5, min(per_page, 200))
    offset = (page - 1) * per_page
    base_qs = build_base_qs()

    where = []
    params = []

    if q:
        if q.isdigit():
            where.append("p.cid = ?")
            params.append(int(q))
        else:
            where.append("p.phyto_name LIKE ?")
            params.append(f"%{q}%")

    if only_cid:
        where.append("p.cid IS NOT NULL")

    # strict filters
    if only_3d:
        where.append("COALESCE(s.has_3d, 0) = 1")

    if only_2d:
        where.append("COALESCE(s.has_2d, 0) = 1 AND COALESCE(s.has_3d, 0) = 0")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    total = query_db(
        f"""
        SELECT COUNT(*) AS c
        FROM phytochemicals p
        LEFT JOIN structures s ON s.phyto_id = p.phyto_id
        {where_sql}
        """,
        tuple(params),
        one=True,
    )["c"]

    rows = query_db(
        f"""
        SELECT
            p.phyto_id,
            p.phyto_name,
            p.cid,
            COALESCE(s.has_2d, 0) AS has_2d,
            COALESCE(s.has_3d, 0) AS has_3d,
            (SELECT COUNT(*) FROM spice_phytochemicals sp WHERE sp.phyto_id = p.phyto_id) AS spice_count
        FROM phytochemicals p
        LEFT JOIN structures s ON s.phyto_id = p.phyto_id
        {where_sql}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
        """,
        tuple(params + [per_page, offset]),
    )

    return render_template(
        "browse_phytochemicals.html",
        phytochemicals=rows,
        q=q,
        only_cid=only_cid,
        only_3d=only_3d,
        only_2d=only_2d,
        sort=sort,
        page=page,
        per_page=per_page,
        total=total,
        base_qs=base_qs,
    )


@app.route("/api/search/phytochemicals")
def api_search_phytochemicals():
    q = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 20)), 50)
    if not q:
        return jsonify([])

    if q.isdigit():
        rows = query_db(
            """
            SELECT p.phyto_id, p.phyto_name, p.cid,
                   COALESCE(s.has_2d,0) AS has_2d,
                   COALESCE(s.has_3d,0) AS has_3d
            FROM phytochemicals p
            LEFT JOIN structures s ON s.phyto_id = p.phyto_id
            WHERE p.cid = ?
            ORDER BY p.phyto_name ASC
            LIMIT ?
            """,
            (int(q), limit),
        )
    else:
        rows = query_db(
            """
            SELECT p.phyto_id, p.phyto_name, p.cid,
                   COALESCE(s.has_2d,0) AS has_2d,
                   COALESCE(s.has_3d,0) AS has_3d
            FROM phytochemicals p
            LEFT JOIN structures s ON s.phyto_id = p.phyto_id
            WHERE p.phyto_name LIKE ?
            ORDER BY p.phyto_name ASC
            LIMIT ?
            """,
            (f"{q}%", limit),   # ✅ prefix match
        )

    return jsonify([
        {"phyto_id": r["phyto_id"], "phyto_name": r["phyto_name"], "cid": r["cid"],
         "has_2d": r["has_2d"], "has_3d": r["has_3d"]}
        for r in rows
    ])

# -------------------------
# Detail Pages
# -------------------------

@app.route("/spice/<int:spice_id>")
def spice_detail(spice_id: int):
    spice = query_db(
        "SELECT spice_id, spice_name, botanical_name FROM spices WHERE spice_id = ?",
        (spice_id,),
        one=True,
    )
    if not spice:
        abort(404)

    phytos = query_db(
        """
        SELECT p.phyto_id, p.phyto_name, p.cid,
               COALESCE(s.has_2d,0) AS has_2d,
               COALESCE(s.has_3d,0) AS has_3d
        FROM spice_phytochemicals sp
        JOIN phytochemicals p ON p.phyto_id = sp.phyto_id
        LEFT JOIN structures s ON s.phyto_id = p.phyto_id
        WHERE sp.spice_id = ?
        ORDER BY p.phyto_name ASC
        """,
        (spice_id,),
    )

    return render_template("spice_detail.html", spice=spice, phytochemicals=phytos)


def fetch_phyto_full(phyto_id: int):
    """One unified query used in phytochemical_detail."""
    return query_db(
        """
        SELECT p.phyto_id, p.phyto_name, p.cid,
               COALESCE(s.has_2d,0) AS has_2d,
               COALESCE(s.has_3d,0) AS has_3d,
               s.sdf_2d_path, s.sdf_3d_path, s.png_2d_path,

               d.molecular_formula,
               d.molecular_weight,
               d.xlogp,
               d.tpsa,
               d.hbd,
               d.hba,
               d.rotatable_bonds,
               d.heavy_atom_count,
               d.complexity,
               d.charge,

               -- ✅ formats
               d.smiles,
               d.isomeric_smiles,
               d.inchi,
               d.inchikey,
               d.iupac_name
        FROM phytochemicals p
        LEFT JOIN structures s ON s.phyto_id = p.phyto_id
        LEFT JOIN descriptors d ON d.phyto_id = p.phyto_id
        WHERE p.phyto_id = ?
        """,
        (phyto_id,),
        one=True,
    )


@app.route("/phytochemical/<int:phyto_id>")
def phytochemical_detail(phyto_id: int):
    phyto = fetch_phyto_full(phyto_id)
    if not phyto:
        abort(404)

    cid = phyto["cid"]

    print("\n========== PHYTO DETAIL DEBUG ==========")
    print("phyto_id:", phyto_id)
    print("cid:", cid)
    print("existing smiles:", phyto["smiles"])
    print("existing inchikey:", phyto["inchikey"])
    print("existing mw:", phyto["molecular_weight"])
    print("=======================================\n")

    # ✅ ON-DEMAND structures (if all missing)
    missing_all_structures = (
        not phyto["sdf_2d_path"]
        and not phyto["sdf_3d_path"]
        and not phyto["png_2d_path"]
    )
    if cid and missing_all_structures:
        fetch_structures_on_demand(phyto_id=phyto_id, cid=int(cid))
        phyto = fetch_phyto_full(phyto_id)

    # ✅ ON-DEMAND descriptors + formats
    missing_desc = (
        phyto["molecular_weight"] is None
        or phyto["smiles"] is None
        or phyto["inchikey"] is None
    )
    if cid and missing_desc:
        fetch_descriptors_from_pubchem(phyto_id=phyto_id, cid=int(cid))
        phyto = fetch_phyto_full(phyto_id)

    drug_rules = calc_druglikeness_rules(phyto) if phyto["molecular_weight"] is not None else None

    spices = query_db(
        """
        SELECT sp.spice_id, sp.spice_name
        FROM spice_phytochemicals map
        JOIN spices sp ON sp.spice_id = map.spice_id
        WHERE map.phyto_id = ?
        ORDER BY sp.spice_name ASC
        """,
        (phyto_id,),
    )

    return render_template(
        "phytochemical_detail.html",
        phyto=phyto,
        spices=spices,
        drug_rules=drug_rules,
    )


# -------------------------
# Batch downloader (optional)
# -------------------------

@app.route("/admin/download_missing_structures")
def admin_download_missing_structures():
    """
    Downloads missing structures in batches.
    Example:
      /admin/download_missing_structures?limit=25
    """
    limit = min(int(request.args.get("limit", 10)), 100)

    rows = query_db(
        """
        SELECT p.phyto_id, p.cid
        FROM phytochemicals p
        LEFT JOIN structures s ON s.phyto_id = p.phyto_id
        WHERE p.cid IS NOT NULL AND (
            s.phyto_id IS NULL OR
            (s.sdf_2d_path IS NULL AND s.sdf_3d_path IS NULL AND s.png_2d_path IS NULL)
        )
        LIMIT ?
        """,
        (limit,),
    )

    done = 0
    for r in rows:
        fetch_structures_on_demand(int(r["phyto_id"]), int(r["cid"]))
        done += 1

    return jsonify({"downloaded": done, "requested": limit})


# -------------------------
# Error Page
# -------------------------

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404


if __name__ == "__main__":
    ensure_descriptors_table_schema()
    app.run(debug=True)
