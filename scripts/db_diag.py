import sqlite3
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    db_path = repo_root / "qn_hydrogen_monitor.db"

    print(f"repo_root: {repo_root}")
    print(f"db_path:   {db_path}")
    if not db_path.exists():
        print("DB file NOT found.")
        return
    print(f"DB size:   {db_path.stat().st_size} bytes")

    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.DatabaseError as e:
        print(f"ERROR opening DB: {e}")
        return
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    tables = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    print("tables:", [r[0] for r in tables])

    def count(table: str) -> None:
        try:
            c = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{table} count:", c)
        except Exception as e:
            print(f"{table} error:", e)

    for t in ["articles", "projects_classic"]:
        count(t)

    try:
        rows = cur.execute(
            """
            SELECT id, article_title, project_name, published_at, is_ai_improved
            FROM projects_classic
            ORDER BY id DESC
            LIMIT 5
            """
        ).fetchall()
        print("latest projects_classic rows (id, title, name, date, ai):")
        for r in rows:
            print(dict(r))
    except Exception as e:
        print("projects_classic sample error:", e)

    conn.close()


if __name__ == "__main__":
    main()
