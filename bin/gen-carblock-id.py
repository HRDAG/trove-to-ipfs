#!/usr/bin/env python
#
# Author: Patrick Ball <pball@hrdag.org>
# Maintainer: Patrick Ball <pball@hrdag.org>
# Date: 2025.01.13
# Copyright: HRDAG, GPL-2 or newer
#
# trove-to-ipfs/bin/gen-carblock-id.py

import argparse
import logging
from pathlib import Path
from types import SimpleNamespace
import tomllib as toml

# gotta pip these
import matplotlib.pyplot as plt
import pandas as pd
import sqlalchemy as sa


def getargs() -> SimpleNamespace:
    parser = argparse.ArgumentParser(description="Blocking files into 100MB cars")
    parser.add_argument(
        "-c",
        "--creds",
        help="Path to the psql credentials file",
        default=f"{str(Path.home())}/creds/psql.toml",
    )
    parser.add_argument(
        "-b", "--blocksize", help="size in MB of a single car", default=500, type=int
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        help="where to write descriptive results",
        default="output/",
    )
    args = parser.parse_args()
    with open(args.creds, "rb") as f:
        creds = toml.load(f)
    creds.update(vars(args))
    return SimpleNamespace(**creds)


def getlogger(args: SimpleNamespace) -> logging.Logger:
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s[%(levelname)s]: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
    )
    file_handler = logging.FileHandler(f"{args.outputdir}/add-carblocks.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info("logger setup")
    return logger


if __name__ == "__main__":
    args = getargs()
    print(args)
    logger = getlogger(args)
    engine = sa.create_engine(
        f"postgresql://{args.user}:{args.password}@localhost:5432/mydb"
    )

    logger.info("reading db to df")
    with engine.connect() as con:
        df = pd.read_sql_table("fs", con)
    logger.info(f"connection to db ok with {len(df)} recs")

    df["csize"] = df["dfize"].cumsum()
    blocksize = args.blocksize * 1024 * 1024  # convert to MB
    df["carblock"] = df["csize"].div(blocksize).astype(int)
    logger.info("carblocks made, adding column")

    with engine.connect() as con:
        drop_c = sa.text("ALTER TABLE fs DROP COLUMN IF EXISTS carblock;")
        con.execute(drop_c)
        con.commit()
        add_c = sa.text("ALTER TABLE fs ADD COLUMN carblock INT;")
        con.execute(add_c)
        con.commit()

    df["block_size"] = df.groupby(["carblock"])["fsize"].transform("sum")
    block_sizes = df.groupby("carblock").first()

    bins = [x * 1e7 for x in range(5, 20)]
    ax = block_sizes.block_size.plot.hist(bins=bins)
    fig = ax.get_figure()
    fig.savefig(f"{args.outputdir}/blocksizes-hist.png")
    plt.close(fig)

    logger.info("writing column to sql")
    df.drop(["csize", "block_size"], axis=1, inplace=True)
    df.to_sql("fs", con=engine, if_exists="replace", index=False, chunksize=10000)
    logger.info("rewrote sql table")

# done.
