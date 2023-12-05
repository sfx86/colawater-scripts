from pathlib import Path

import polars as pl
import polars.selectors as cs

EXPORT_PATH = Path("./export").resolve()
OUTPUT_PATH = Path("./output").resolve()
# path relative to export/output
ALIASES = {
    "gm": "gravity_main.csv",
    "mh": "manhole.csv",
    "pm": "pressurized_main.csv",
    "ps": "pump_station.csv",
}

INPUTS = {alias: EXPORT_PATH / filename for alias, filename in ALIASES.items()}
OUTPUTS = {alias: OUTPUT_PATH / filename for alias, filename in ALIASES.items()}

SUBBASINS = pl.DataFrame(
    {
        "subbasin": [
            "BR01",
            "BR02",
            "BR03",
            "BR04",
            "CC01",
            "CC02",
            "CC03",
            "CC04",
            "CC05",
            "CC06",
            "CC07",
            "CC08",
            "CC09",
            "CC10",
            "CC11",
            "CC12",
            "CC21",
            "GC01",
            "GC02",
            "GC03",
            "GC04",
            "GC05",
            "GC06",
            "GC07",
            "GC08",
            "GC09",
            "GC10",
            "GC11",
            "GC12",
            "GC13",
            "GC14",
            "GC15",
            "GC16",
            "GC17",
            "GC18",
            "MC01",
            "MC02",
            "MC03",
            "MC04",
            "MC05",
            "RB01",
            "RB02",
            "RB03",
            "RB04",
            "RB05",
            "RB06",
            "RB07",
            "RB08",
            "SR01",
            "SR02",
            "SR03",
            "SR04",
            "SR05",
            "SR06",
            "SR07",
            "SR08",
            "SR09",
            "SR10",
            "SR11",
            "SR12",
            "SR13",
            "SR14",
            "SR15",
            "SB01",
            "SB02",
            "SB03",
            "SB04",
            "SB05",
            "SB06",
            "WC01",
            "WC02",
            "",  # for features not in a subbasin
        ]
    }
)


# helper expressions
def subbasin() -> pl.Expr:
    return pl.col("SUBBASINID").alias("subbasin")


def Shape_Length() -> pl.Expr:
    return pl.col("Shape_Length").alias("length")


def SHAPE_Length() -> pl.Expr:
    return pl.col("SHAPE_Length").alias("length")


def is_active_city() -> pl.Expr:
    return (pl.col("LIFECYCLESTATUS") == "Active") & (pl.col("OWNEDBY") == 1)


def diameter() -> pl.Expr:
    return pl.col("DIAMETER").alias("diameter")


def d() -> pl.Expr:
    return pl.col("diameter")


def length() -> pl.Expr:
    return pl.col("length")


def main() -> None:
    gm_lf = pl.scan_csv(INPUTS["gm"])
    mh_lf = pl.scan_csv(INPUTS["mh"])
    pm_lf = pl.scan_csv(INPUTS["pm"])
    ps_lf = pl.scan_csv(INPUTS["ps"])

    gm_df = (
        gm_lf.filter(is_active_city())
        .select(
            diameter(),
            subbasin(),
            Shape_Length(),
        )
        .group_by("subbasin")
        .agg(
            length().filter(d().is_between(1, 15)).sum().alias("small"),
            length().filter(d() >= 15).sum().alias("large"),
            length().filter(d().is_null() | (d() <= 0)).sum().alias("null/unk"),
        )
        .collect()
    )
    mh_df = (
        mh_lf.filter(is_active_city())
        .select(subbasin())
        .group_by("subbasin")
        .count()
        .sort("subbasin")
        .collect()
    )
    pm_df = (
        pm_lf.filter(is_active_city())
        .select(
            subbasin(),
            SHAPE_Length(),
        )
        .group_by("subbasin")
        .agg(pl.col("length").sum())
        .sort("subbasin")
        .collect()
    )
    ps_df = (
        ps_lf.filter(is_active_city())
        .select(subbasin())
        .group_by("subbasin")
        .count()
        .sort("subbasin")
        .collect()
    )

    for alias, df in zip(sorted(ALIASES.keys()), (gm_df, mh_df, pm_df, ps_df)):
        SUBBASINS.join(df, on="subbasin", how="left").write_csv(OUTPUTS[alias])


if __name__ == "__main__":
    main()
