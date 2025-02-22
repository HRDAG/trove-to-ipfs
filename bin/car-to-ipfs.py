#!/usr/bin/env python
#
# Author: Patrick Ball <pball@hrdag.org>
# Maintainer: Patrick Ball <pball@hrdag.org>
# Date: 2025-02-02
# Copyright: HRDAG, GPL-2 or newer
#
# trove-to-ipfs/bin/org-files-to-car.py

import argparse
from functools import partial
import filecmp
import gzip
import logging
from pathlib import Path
from operator import itemgetter
import os
import random
import signal
import subprocess
import tempfile
import time
import tomllib as toml
import shutil
from typing import Tuple

# --- these are not part of the std library
import psycopg  # noqa: E402
import requests

global logger
DEBUG = True
sr = partial(subprocess.run, text=True, capture_output=True)


def signal_handler(sig, frame):
    raise AssertionError("SIGINT caught")


# from perplexity.ai
def get_dir_size_no_recursion(path: Path | str) -> float:
    bytes = sum(
        os.path.getsize(os.path.join(path, f))
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f))
    )
    return round((bytes / (1024 * 1024.0)), 1)


def getargs() -> argparse.Namespace:
    """getting arguments and keeping track of globals"""
    parser = argparse.ArgumentParser(description="simple description")
    credsfile = f"{str(Path.home())}/creds/psql.toml"
    parser.add_argument(
        "-c", "--creds", help="Path to the postgres credentials file", default=credsfile
    )
    w3file = f"{str(Path.home())}/creds/w3.toml"
    parser.add_argument(
        "-w", "--w3creds", help="Path to the w3 credentials file", default=w3file
    )
    parser.add_argument(
        "-n",
        "--num_carblocks",
        help="how many carblocks to process (x=forever)",
        default=-1,
        type=int,
    )
    parser.add_argument(
        "-r",
        "--check_fraction",
        help="[0-1] what fraction of carfiles to check",
        default=1,
        type=float,
    )
    parser.add_argument(
        "-l",
        "--debug_limit",
        help="limit on number of files to process (for debugging)",
        default=None,
        type=int,
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
        host="localhost",
        user=creds["user"],
        password=creds["password"],
        row_factory=psycopg.rows.namedtuple_row,
        dbname=creds["dbname"],
        port=5432,
    )
    setattr(args, "conn", conn)
    del creds

    with open(args.w3creds, "rb") as f:
        creds = toml.load(f)
    setattr(args, "w3email", creds["w3email"])
    setattr(args, "space_did", creds["space_did"])
    setattr(args, "user_did", creds["user_did"])
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


def get_carblocks(args: argparse.Namespace) -> list:
    select_q = """SELECT DISTINCT carblock FROM fs
        WHERE blocked_tm IS NULL AND
            uploaded_tm IS NULL;"""
    with args.conn.cursor() as cur:
        cur.execute(select_q)
        # NB: itemgetter is a little more efficient for large lists
        carblocks = list(map(itemgetter(0), cur.fetchall()))
    logger.info(f"found {len(carblocks)} carblocks")
    return carblocks


def lock_carblock_files(args: argparse.Namespace, carblock: int) -> bool:
    logger.debug(f"carblock is {carblock} and is type {type(carblock)}")
    now = int(time.time())
    with args.conn.cursor() as cur:
        cur.execute(
            """
                    UPDATE fs
                    SET blocked_tm = to_timestamp(%s)
                    WHERE carblock = %s
                    """,
            (now, carblock),
        )
        logger.info(
            f"{cur.rowcount} rows (carblock={carblock}) updtd with blocked_tm={now}"
        )
        args.conn.commit()
    return True


def rollback_carblock_lock(
    args: argparse.Namespace, carblock: int, cardir: Path | None
) -> bool:
    if not DEBUG:
        with args.conn.cursor() as cur:
            cur.execute(
                """
                        UPDATE fs
                        SET blocked_tm = NULL
                        WHERE carblock = %s AND car_url is NULL;
                        """,
                (carblock,),
            )
            logger.error(f"{cur.rowcount} rows rolled back to NULL time")
            args.conn.commit()
        try:
            shutil.rmtree(str(cardir))
        except FileNotFoundError:  # other errors raised but ignores FileNotFound
            pass
    return True


def get_filenames(
    args: argparse.Namespace, carblock: int, check: bool = False
) -> Tuple[list, list]:
    with args.conn.cursor() as cur:
        cur.execute(
            """
                    SELECT pth,fname,blocked_tm
                    FROM fs
                    WHERE carblock = %s AND car_url IS NULL;
                    """,
            (carblock,),
        )
        fetched = cur.fetchall()
    # NB if fetched is empty, the next line ends the run
    logger.debug(f"(carblock={carblock}), first row is {fetched[0]}")
    assert all(x.blocked_tm is not None for x in fetched)
    files = [Path(r.pth, r.fname) for r in fetched]
    ftuples = [(r.pth, r.fname) for r in fetched]
    logger.info(
        f"{len(files)} not-uploaded files in carblock={carblock} to be uploaded"
    )
    if check:
        for p in files:
            try:
                if not p.exists():
                    logger.warn(
                        f"{str(p)} does not exist in filesystem, skipping carblock={carblock}"
                    )
                    return list(), list()
            except OSError:
                logger.warn(
                    f"{str(p)} raises OSError (disk failure?), skipping carblock={carblock}"
                )
                return list(), list()
        logger.debug(f"all {len(files)} in carblock={carblock} exist in filesystem")
    if args.debug_limit:
        return files[0 : args.debug_limit], ftuples[0 : args.debug_limit]
    else:
        return files, ftuples


def cp_files_tmp(files: list, carblock: int) -> Tuple[Path, Path]:
    tmproot = "/var/tmp"
    os.makedirs(tmproot, exist_ok=True)
    cardir = Path(tempfile.mkdtemp(dir=tmproot))
    for f in files:
        gzip_name = f"{f.name}.gz"
        with open(f, "rb") as f_in:
            with gzip.open(cardir / gzip_name, "wb") as f_out:
                f_out.writelines(f_in)
    carpth = Path(f"{str(cardir)}.car")
    mb = get_dir_size_no_recursion(cardir)
    logger.info(
        f"from (carblock={carblock}), {len(files)} files ({mb}MB) copied to {cardir}"
    )
    return cardir, carpth


def w3setup(args: argparse.Namespace) -> bool:
    result = sr(["w3", "login", args.w3email], timeout=10)
    assert "Agent was authorized" in result.stdout and result.returncode == 0
    logger.debug(f"{result.stdout.strip()}")

    result = sr(["w3", "whoami", args.w3email])
    assert args.user_did in result.stdout and result.returncode == 0
    logger.debug(f"{result.stdout.strip()}")

    result = sr(["w3", "space", "use", args.space_did])
    assert result.returncode == 0
    logger.debug(f"{result.stdout.strip()}")
    return True


def pack_car(cardir: Path, carpth: Path) -> str:
    result = sr(["npx", "ipfs-car", "pack", cardir, "--output", carpth])
    if result.returncode != 0:
        logger.critical(f"ipfs-car failed {str(result)}")
        raise AssertionError
    logger.debug(f"ipfs-car pack stdout {result.stdout.strip()}")
    logger.debug(f"ipfs-car pack stderr {result.stderr.strip()}")
    return result.stderr.strip()


def upload_car(carpth: Path, car_cid: str) -> str:
    attempt = 1
    while True:
        if attempt > 3:
            logger.warning(f"w3 up failed {str(result)}, attempt={attempt}")
            logger.critical("no more attempts, giving up.")
            raise AssertionError
        result = sr(["w3", "up", "--no-wrap", "--car", carpth])
        if result.returncode == 0:
            break
        logger.warning(f"w3 up failed {str(result)}, attempt={attempt}")
        attempt += 1
    car_url = result.stdout.strip()[2:]
    if not (car_url.startswith("https://") and car_url.endswith(car_cid)):
        logger.critical(f"car_url={car_url}, upload FAIL")
        raise AssertionError
    logger.debug(f"w3 up returned {car_url}")
    return car_url


def update_url_in_db(ftuples: list, car_url: str) -> int:
    query = """UPDATE fs
                SET uploaded_tm = to_timestamp(%s),
                    car_url = %s
                WHERE (pth, fname) = (%s, %s)
                """
    update_data = [(int(time.time()), car_url, pth, fname) for pth, fname in ftuples]
    with args.conn.cursor() as cur:
        cur.executemany(query, update_data)
        rowcount = cur.rowcount
    args.conn.commit()
    logger.info(f"updated uploaded_tm, car_url in {rowcount} rows.")
    return rowcount


def test_car(ftuples: list, cardir: Path, carblock: int, car_url: str) -> None:
    _, testfile = random.choice(ftuples)
    gzpath = cardir / f"{testfile}.gz"
    ipfsurl = f"{car_url}/{testfile}.gz"
    response = requests.get(ipfsurl)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    with open(tmp.name, "wb") as f:
        f.write(response.content)
    if not filecmp.cmp(gzpath, tmp.name, shallow=False):
        logger.critical(f"{str(gzpath)} does not match copy in IPFS")
        raise AssertionError
    Path(tmp.name).unlink()
    logger.info(f"chk OK: from carblock={carblock}, {str(gzpath)} matches upload")


def up_one_carblock(args: argparse.Namespace) -> bool:
    carblocks = get_carblocks(args)
    carblock = carblocks[0]
    cardir = None
    try:
        lock_carblock_files(args, carblock)
        files, ftuples = get_filenames(args, carblock, check=True)
        if len(files) == 0:
            return False
        cardir, carpth = cp_files_tmp(files, carblock)
        car_cid = pack_car(cardir, carpth)
        car_url = upload_car(carpth, car_cid)
        rowcount = update_url_in_db(ftuples, car_url)
        assert rowcount == len(files)

        if random.random() <= args.check_fraction:
            test_car(ftuples, cardir, carblock, car_url)
        else:
            logger.info("prob too low, no download test conducted.")
        # cleanup at shell:
        # for x in /var/tmp/tmp*.car ; do rm -r ${x%.*}; rm $x; done
        shutil.rmtree(cardir)
        carpth.unlink()
        logger.debug(f"{cardir} removed.")
    except:  # noqa: E722
        rollback_carblock_lock(args, carblock, cardir)
        raise
    logger.info(f"carblock={carblock} uploaded successfully to {car_url}, done.")
    return True


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    args = getargs()
    logger = getlogger(args)
    if args.num_carblocks < 1:
        logger.warning("no num_carblocks set, will run until there are no more.")
    w3setup(args)

    run_n = args.num_carblocks if args.num_carblocks >= 1 else 10000
    for i in range(run_n):
        up_one_carblock(args)

# done.
