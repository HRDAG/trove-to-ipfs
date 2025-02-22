#!/usr/bin/env python
#
# Author: Patrick Ball <pball@hrdag.org>
# Maintainer: Patrick Ball <pball@hrdag.org>
# Date: 2025-02-14
# Copyright: HRDAG, GPL-2 or newer
#
# trove-to-ipfs/bin/add_file_cids_pg.py

import argparse
import json
import logging
from pathlib import Path
import tomllib as toml
from typing import List, NamedTuple

# --- these are not part of the std library
import psycopg  # noqa: E402
from psycopg import sql

global logger
DEBUG = True


class FileCID(NamedTuple):
    """this is the data structure that comes from `ipfs dag get $CID`"""

    hash: str
    name: str
    tsize: int


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
        default=f"{YOURTMPPATH}/car_did.txt",
    )
    parser.add_argument(
        "-t",
        "--tmpdir",
        help="Path to dir of json files from ipfs dag get",
        default=f"{YOURTMPPATH}/car-did-json",
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


def json2tbl(tmpdir: Path | str, cid: str) -> List[FileCID]:
    """parse a json returned from `ipfs dag get $CID`
    to a list of FileCID namedtuples"""
    fname = f"{tmpdir}/{cid}.json"
    with open(fname, "rb") as f:
        js = json.load(f)

    return [
        FileCID(hash=r["Hash"]["/"], name=r["Name"], tsize=r["Tsize"])
        for r in js["Links"]
    ]


def prox_1_car_cid(args: argparse.Namespace, car_cid: str):
    """iterate over all cids, write the file's cid into db"""
    recs = json2tbl(args.tmpdir, car_cid)
    car_url = f"https://w3s.link/ipfs/{car_cid}"
    chgd = 0
    for rec in recs:
        fname = rec.name[:-3]  # trims the .gz suffix
        logger.debug(f"rec={rec}, fname={fname}")
        query = sql.SQL("""
            UPDATE fs
            SET tsize = {}, file_cid = {}
            WHERE fname = {} AND car_url = {}
        """).format(
            sql.Literal(rec.tsize),
            sql.Literal(rec.hash),
            sql.Literal(fname),
            sql.Literal(car_url),
        )
        with args.conn.cursor() as cur:
            cur.execute(query)
            chgd += cur.rowcount

    args.conn.commit()
    logger.info(f"processed {len(recs)} json / {chgd} rows for car_cid={car_cid}")


if __name__ == "__main__":
    args = getargs()
    logger = getlogger(args)

    with args.conn.cursor() as cur:
        cur.execute("SELECT DISTINCT car_url from fs;")
        car_urls = [r.car_url for r in cur.fetchall()]
    logger.info(f"retrieved {len(car_urls)} car_urls")

    hdr = "https://w3s.link/ipfs/"
    assert all([u.startswith(hdr) for u in car_urls])
    cids = set([c[len(hdr) :] for c in car_urls])  # trim hdr

    # these came from `w3 ls` in the IPFS space
    with open(args.w3ls, "rt") as f:
        w3ls_cids = set(line.strip() for line in f)
    logger.info(f"w3ls found {len(w3ls_cids)} cids in space (some were tests)")

    assert cids <= w3ls_cids
    logger.info("OK: all car cids in database are in w3ls")

    # filter cids for the ones that are already processed in previous runs
    with args.conn.cursor() as cur:
        cur.execute("SELECT DISTINCT car_url from fs WHERE tsize IS NULL;")
        car_urls = [r.car_url for r in cur.fetchall()]
    cids = set([c[len(hdr) :] for c in car_urls])  # trim hdr
    logger.info(f"{len(cids)} cids found with null tsize, will be processed")

    for car_cid in cids:
        prox_1_car_cid(args, car_cid)

    args.conn.close()

# done.
