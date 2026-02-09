"""
Download 2D/3D SDF and 2D PNG depictions from PubChem for phytochemicals that have a CID.

- Reads SQLite DB
- Downloads into ./static/structures/{sdf2d,sdf3d,png}
- Updates relative paths in DB (relative to Flask /static)
  Example: structures/png/CID_2758.png

Usage examples:
    py -3 download_structures.py --db data/spices.db --out static/structures --limit 100
    py -3 download_structures.py --db data/spices.db --mode backfill2d
    py -3 download_structures.py --db data/spices.db --mode all --sleep 0.3

Notes:
- PubChem rate limits: this script uses delays and retries.
"""

import argparse
import sqlite3
import time
from pathlib import Path
from urllib.error import URLError, HTTPError
import urllib.request

PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/CID/{cid}/{fmt}"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SpicesDB/1.0"


# -------------------------
# Helpers
# -------------------------


def download(url: str, dest: Path, retries: int = 3, timeout: int = 30) -> bool:
    """Download URL to dest with retries."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()

            if not data:
                raise URLError("empty response")

            dest.write_bytes(data)
            return True

        except (HTTPError, URLError, TimeoutError) as e:
            if i == retries - 1:
                print(f"❌ Failed: {url} -> {dest.name} ({e})")
                return False
            time.sleep(1.5 * (i + 1))

    return False


def rel_to_static(dest: Path) -> str:
    """
    Convert absolute/relative file path into Flask static-relative path.
    Example:
        static/structures/png/CID_2758.png -> structures/png/CID_2758.png
    """
    parts = list(dest.parts)
    if "static" in parts:
        idx = parts.index("static")
        rel_parts = parts[idx + 1 :]
        return "/".join(rel_parts)
    return str(dest).replace("\\", "/")


# -------------------------
# Main
# -------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/spices.db", help="SQLite DB path")
    ap.add_argument("--out", default="static/structures", help="Output base folder")
    ap.add_argument("--limit", type=int, default=0, help="0 means no limit")
    ap.add_argument("--sleep", type=float, default=0.25, help="Delay between requests")
    ap.add_argument(
        "--mode",
        choices=["missing", "all", "backfill2d"],
        default="missing",
        help=(
            "missing = download only missing files (default)\n"
            "all = force re-download everything\n"
            "backfill2d = download 2D+PNG for records that have 3D but missing 2D/PNG"
        ),
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    out_dir = Path(args.out)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON")

    # NOTE: some older DBs may not have a structures row for each phyto.
    # We'll join + require structures. If missing, you should create structures rows in load_data.py.
    rows = con.execute(
        """
        SELECT p.phyto_id, p.cid,
               COALESCE(s.has_2d, 0) as has_2d,
               COALESCE(s.has_3d, 0) as has_3d,
               COALESCE(s.sdf_2d_path,''), COALESCE(s.sdf_3d_path,''), COALESCE(s.png_2d_path,'')
        FROM phytochemicals p
        JOIN structures s ON s.phyto_id = p.phyto_id
        WHERE p.cid IS NOT NULL
        ORDER BY p.cid
        """
    ).fetchall()

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    ok2d = ok3d = okpng = 0
    attempted = 0

    print(f"✅ Mode: {args.mode}")
    print(f"✅ Rows with CID: {len(rows)}")
    print(f"✅ Output directory: {out_dir.resolve()}\n")

    for phyto_id, cid, has_2d, has_3d, sdf2d_path, sdf3d_path, png_path in rows:
        attempted += 1
        cid = int(cid)

        # Decide downloads based on mode
        force = (args.mode == "all")

        need_2d = force or (not sdf2d_path)
        need_3d = force or (not sdf3d_path)
        need_png = force or (not png_path)

        # backfill2d mode = only add 2D+PNG when 3D exists
        if args.mode == "backfill2d":
            # only process 3D-available compounds
            if not has_3d:
                continue
            need_3d = False  # don't touch 3D
            need_2d = (not sdf2d_path)
            need_png = (not png_path)

        # -------------------------
        # 2D SDF
        # Rule: if compound has CID, we can always fetch 2D SDF.
        # Also: if 3D exists, we still want 2D.
        # -------------------------
        if need_2d:
            url = PUBCHEM_BASE.format(cid=cid, fmt="SDF?record_type=2d")
            dest = out_dir / "sdf2d" / f"CID_{cid}.sdf"
            if download(url, dest):
                con.execute(
                    "UPDATE structures SET sdf_2d_path=?, has_2d=1 WHERE phyto_id=?",
                    (rel_to_static(dest), phyto_id),
                )
                ok2d += 1
            time.sleep(args.sleep)

        # -------------------------
        # 3D SDF
        # Rule: only attempt 3D download if has_3d is expected OR mode=all
        # (Some CIDs may not have 3D in PubChem)
        # -------------------------
        if need_3d and (has_3d == 1 or force):
            url = PUBCHEM_BASE.format(cid=cid, fmt="SDF?record_type=3d")
            dest = out_dir / "sdf3d" / f"CID_{cid}.sdf"
            if download(url, dest):
                con.execute(
                    "UPDATE structures SET sdf_3d_path=?, has_3d=1 WHERE phyto_id=?",
                    (rel_to_static(dest), phyto_id),
                )
                ok3d += 1
            time.sleep(args.sleep)

        # -------------------------
        # PNG depiction
        # Always useful (even if only 3D exists)
        # -------------------------
        if need_png:
            url = PUBCHEM_BASE.format(cid=cid, fmt="PNG")
            dest = out_dir / "png" / f"CID_{cid}.png"
            if download(url, dest):
                con.execute(
                    "UPDATE structures SET png_2d_path=? WHERE phyto_id=?",
                    (rel_to_static(dest), phyto_id),
                )
                okpng += 1
            time.sleep(args.sleep)

        # commit periodically
        if attempted % 50 == 0:
            con.commit()
            print(f"...progress: {attempted}/{len(rows)}")

    con.commit()
    con.close()

    print("\n✅ DONE")
    print(f"Attempted rows: {attempted}")
    print(f"Downloaded: 2D SDF={ok2d}, 3D SDF={ok3d}, PNG={okpng}")


if __name__ == "__main__":
    main()
