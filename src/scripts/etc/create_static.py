import argparse
from pathlib import Path
from typing import NoReturn, Union

import arcpy


def main(args: argparse.Namespace) -> None:
    inputs = {
        "GIS.SDE.Boundary": [
            "GIS.SDE.COC_CITY_LIMIT",
            "GIS.SDE.COUNCIL_DISTRICT",
            "GIS.SDE.COUNTY",
            "GIS.SDE.SC_Counties",
            "GIS.SDE.SC_Counties_Generalized",
        ],
        "GIS.SDE.CensusData": [
            "GIS.SDE.CensusBlockGroups_2010",
            "gis.SDE.CensusBlockGroups_2020",
            "GIS.SDE.CensusBlocks_2010",
            "gis.SDE.CensusBlocks_2020",
            "GIS.SDE.CensusTracts_2010",
            "gis.SDE.CensusTracts_2020",
        ],
        "GIS.SDE.LandRecords": [
            "GIS.SDE.ADDRESS_POINT",
            "GIS.SDE.BUILDING_FOOTPRINT",
            "GIS.SDE.PARCEL",
        ],
        "GIS.SDE.Transportation": [
            "GIS.SDE.STREETS",
            "GIS.SDE.TR_RAILROAD",
        ],
    }

    input_features = [f"{fd}\\{fc}" for fd, fcs in inputs.items() for fc in fcs]
    output_geodatabase = Path(args.geodatabase).resolve()

    if not output_geodatabase.exists():
        print("Creating output geodatabase...")
        arcpy.management.CreateFileGDB(  # pyright: ignore [reportGeneralTypeIssues]
            str(output_geodatabase.parent),
            str(output_geodatabase.name),
        )

    print("Downloading feature classes from cypress...")
    arcpy.env.workspace = str(  # pyright: ignore [reportGeneralTypeIssues]
        Path(args.cypress).resolve()
    )
    arcpy.conversion.FeatureClassToGeodatabase(  # pyright: ignore [reportGeneralTypeIssues]
        input_features,
        str(output_geodatabase),
    )


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Creates a new local copy of often used, but rarely updated layers from cypress."
    )
    parser.add_argument(
        "-c",
        "--cypress",
        dest="cypress",
        type=SDE,
        help="The path to a connection file for cypress.",
    )
    parser.add_argument(
        "-g",
        "--geodatabase",
        default="./static.gdb",
        help="The output geodatabase.",
    )

    return parser.parse_args()


def SDE(filename: str) -> Union[str, NoReturn]:
    if not filename.lower().endswith(".sde"):
        raise ValueError(f"Expected an SDE file, got '{filename}'.")

    return filename


if __name__ == "__main__":
    main(args())
