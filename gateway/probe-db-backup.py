"""프로브 DB 일일 백업 — 최근 7개만 남긴다

설치: /opt/lora-probe/probe-db-backup.py (probe-db-backup.timer 가 매일 실행)
백업: /var/lib/lora-probe/backup/probe_client_YYYYMMDD.db.gz

lora-probe 가 쓰는 중에도 일관된 사본을 얻으려고 SQLite 온라인 백업 API 를 쓴다.
파일 복사(cp)는 쓰기 도중이면 깨진 사본이 나올 수 있다. 원본은 읽기 전용으로 연다.
"""
import glob
import gzip
import os
import shutil
import sqlite3
import time

SRC = os.getenv("BACKUP_SRC", "/var/lib/lora-probe/probe_client.db")
DST_DIR = os.getenv("BACKUP_DIR", "/var/lib/lora-probe/backup")
KEEP = int(os.getenv("BACKUP_KEEP", "7"))


def main():
    os.makedirs(DST_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d")
    tmp = os.path.join(DST_DIR, f".probe_client_{stamp}.db")
    final = os.path.join(DST_DIR, f"probe_client_{stamp}.db.gz")

    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True, timeout=30)
    dst = sqlite3.connect(tmp)
    src.backup(dst, pages=1024, sleep=0.05)    # 조금씩 나눠 복사해 기록 스레드를 오래 막지 않는다
    n = dst.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
    dst.close()
    src.close()

    with open(tmp, "rb") as f, gzip.open(final + ".part", "wb", compresslevel=6) as g:
        shutil.copyfileobj(f, g)
    os.replace(final + ".part", final)
    os.remove(tmp)

    backups = sorted(glob.glob(os.path.join(DST_DIR, "probe_client_*.db.gz")))
    for old in backups[:-KEEP]:
        os.remove(old)
    print(f"[backup] {final} ({n} rows, {os.path.getsize(final)} bytes), kept {min(len(backups), KEEP)}")


if __name__ == "__main__":
    main()
