#!/usr/bin/env python
#
# Author: Patrick Ball <pball@hrdag.org>
# Maintainer: Patrick Ball <pball@hrdag.org>
# Date: 2025-02-19
# Copyright (c) 2025 GPL-2 HRDAG info@hrdag.org
#
# trove-to-ipfs/bin/add_cids_from_csv.py

import argparse
import logging
import os
from pathlib import Path
import tomllib as toml
from typing import List, Tuple

# --- these are not part of the std library
import psycopg  # noqa: E402

global logger
DEBUG = True


def getargs() -> argparse.Namespace:
    """getting arguments and keeping track of globals"""
    parser = argparse.ArgumentParser(description="simple description")
    credsfile = f"{str(Path.home())}/creds/psql.toml"
    parser.add_argument(
        "-c", "--creds", help="Path to the postgres credentials file", default=credsfile
    )
    parser.add_argument(
        "-w",
        "--w3ls",
        help="Path to file of w3 ls of cids",
        default=f"{YOURTMPDIR}/car_did.txt",
    )
    parser.add_argument(
        "-t",
        "--tmpdir",
        help="Path to dir of csv files from `ipfs ls`",
        default=f"{YOURTMPDIR}/car-did-csv",
    )
    parser.add_argument(
        "-o", "--outputdir", help="directory to write results", default="output/"
    )
    args = parser.parse_args()
    assert Path(args.outputdir).exists()
    print(args)

    with open(args.creds, "rb") as f:
        creds = toml.load(f)
    conn = psycopg.connect(
        autocommit=True,
        host="localhost",
        user=creds["user"],
        password=creds["password"],
        row_factory=psycopg.rows.namedtuple_row,
        dbname="pescados",
        port=5432,
    )
    setattr(args, "conn", conn)
    del creds
    return args


def getlogger(args: argparse.Namespace) -> logging.Logger:
    logger = logging.getLogger("main")
    loglevel = logging.DEBUG
    logger.setLevel(loglevel)
    formatter = logging.Formatter(
        "[%(process)d] %(asctime)s[%(levelname)s]: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logpath = f"{args.outputdir}/{Path(__file__).stem}.log"
    file_handler = logging.FileHandler(logpath, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info("logger setup")
    return logger


def csvs_to_tbl(args: argparse.Namespace) -> List[Tuple[str, str, int, str]]:
    """read all the csvs from `ipfs ls $CID`, parse to list of tuples"""
    with args.conn.cursor() as cur:
        cur.execute("SELECT DISTINCT fname FROM fs WHERE tsize IS NULL;")
        fnames = {r.fname for r in cur.fetchall()}
    logger.info(f"found {len(fnames)} fnames with null tsize")

    valid_fnames = 0
    invalid_fnames = 0
    recs = []
    for fname in os.listdir(args.tmpdir):
        fpath = f"{args.tmpdir}/{fname}"
        if Path(fpath).stat().st_size < 5:
            continue

        with open(fpath, "rt") as f:
            car_url = f"https://w3s.link/ipfs/{fname[:-4]}"
            for line in f.readlines():
                cid, tsize, fname = line.split()
                if not fname.endswith(".gz"):
                    invalid_fnames += 1
                    continue
                fname = fname[:-3]
                if fname in fnames:
                    recs.append(tuple((car_url, fname, int(tsize), cid)))
                    valid_fnames += 1
                else:
                    invalid_fnames += 1
    logger.info(
        f"returned {valid_fnames} valid fnames and {invalid_fnames} invalid fnames"
    )
    return recs


def merge_csvs_to_fs(args: argparse.Namespace, recs: list[tuple]) -> None:
    """with the csvs in a list[tuples], merge them into the fs table"""
    update_q = """
        UPDATE fs f
        SET file_cid = t.file_cid,
            tsize = t.tsize
        FROM toupd t
        WHERE f.car_url = t.car_url
        AND f.fname = t.fname ; """
    create_q = """
        CREATE TABLE toupd (
            car_url VARCHAR(128),
            fname TEXT,
            tsize INTEGER,
            file_cid VARCHAR(60)) ; """
    copy_q = "COPY toupd (car_url, fname, tsize, file_cid) FROM STDIN"

    with args.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fs WHERE file_cid IS NULL;")
        results = cur.fetchall()
        logger.info(f"at start, files without cids: {results}")

        cur.execute("DROP TABLE IF EXISTS toupd;")
        cur.execute(create_q)
        with cur.copy(copy_q) as copy:
            for rec in recs:
                copy.write_row(rec)
        logger.info(f"copied {len(recs)} records to toupd")

        cur.execute(update_q)
        cur.execute("DROP TABLE IF EXISTS toupd;")
        args.conn.commit()
        logger.info("fs updated")

        cur.execute("SELECT COUNT(*) FROM fs WHERE file_cid IS NULL;")
        results = cur.fetchall()
        logger.info(f"after update, files without cids: {results}")


if __name__ == "__main__":
    args = getargs()
    logger = getlogger(args)
    recs = csvs_to_tbl(args)
    merge_csvs_to_fs(args, recs)
    args.conn.close()

# done.
