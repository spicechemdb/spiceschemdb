import sqlite3

DB = "data/spices.db"

def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Find duplicate phyto names
    dups = cur.execute("""
        SELECT phyto_name, COUNT(*) as c
        FROM phytochemicals
        GROUP BY phyto_name
        HAVING c > 1
    """).fetchall()

    print(f"Found {len(dups)} duplicate phytochemical names.\n")

    for d in dups:
        name = d["phyto_name"]

        # all rows for this name
        rows = cur.execute("""
            SELECT p.phyto_id, p.cid,
                (SELECT COUNT(*) FROM spice_phytochemicals sp WHERE sp.phyto_id=p.phyto_id) AS spice_count,
                (SELECT COUNT(*) FROM structures s WHERE s.phyto_id=p.phyto_id) AS struct_count
            FROM phytochemicals p
            WHERE p.phyto_name = ?
            ORDER BY
                (p.cid IS NOT NULL) DESC,
                spice_count DESC,
                struct_count DESC
        """, (name,)).fetchall()

        if len(rows) < 2:
            continue

        keeper = rows[0]
        keeper_id = keeper["phyto_id"]

        dup_ids = [r["phyto_id"] for r in rows[1:]]

        print(f"🔁 {name}")
        print(f"   keeper: {keeper_id} (CID={keeper['cid']} spices={keeper['spice_count']})")
        print(f"   merging: {dup_ids}")

        for dup_id in dup_ids:
            # Move spice mappings
            cur.execute("""
                UPDATE OR IGNORE spice_phytochemicals
                SET phyto_id = ?
                WHERE phyto_id = ?
            """, (keeper_id, dup_id))

            # If structures exist for dup but not keeper, transfer them
            dup_struct = cur.execute("SELECT * FROM structures WHERE phyto_id=?", (dup_id,)).fetchone()
            keeper_struct = cur.execute("SELECT * FROM structures WHERE phyto_id=?", (keeper_id,)).fetchone()

            if dup_struct and not keeper_struct:
                cur.execute("""
                    UPDATE structures
                    SET phyto_id = ?
                    WHERE phyto_id = ?
                """, (keeper_id, dup_id))

            # Delete dup structures (if still any)
            cur.execute("DELETE FROM structures WHERE phyto_id=?", (dup_id,))

            # Delete duplicate phytochemical row
            cur.execute("DELETE FROM phytochemicals WHERE phyto_id=?", (dup_id,))

        conn.commit()
        print("   ✅ merged\n")

    conn.close()
    print("✅ All duplicates processed successfully.")


if __name__ == "__main__":
    main()
