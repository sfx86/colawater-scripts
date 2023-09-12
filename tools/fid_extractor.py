import argparse
import csv
import itertools
import re
from pathlib import Path
from typing import assert_never


def main(args: argparse.Namespace) -> None:
    args.input = [Path(path) for path in args.input]
    args.output = Path(args.output) if args.output is not None else args.output

    filenames = list(
        file.name
        for file in itertools.chain(*(path.iterdir() for path in args.input))
        if file.is_file()
    )

    munder_pattern = lambda pattern: scrape_fids(
        filenames, re.compile(f"({pattern}_{pattern})")
    )

    if args.type == "sewer":
        records = munder_pattern(r"\d{5}MH")
    elif args.type == "stormwater":
        records = munder_pattern(r"\d{6,8}(DP|IN|MP|NS|OD|\d{2})")
    else:
        assert_never(args.type)

    if args.output:
        with open(args.output, "w") as file:
            writer = csv.writer(file, dialect="unix", quoting=csv.QUOTE_ALL)
            writer.writerow(("facility_id", "filename"))
            for row in records:
                writer.writerow(row)

    if args.verbose:
        for fid, filename in records:
            print(f"{fid:15} <- '{filename}'")

    if args.stats:
        num_filenames = len(filenames)
        num_scraped = len([r for r in records if r[0]])
        num_unique = len(set(r[0] for r in records))

        print(f"{num_scraped:n} facility identifiers scraped.")
        print(
            f"{num_filenames - num_scraped:n} facility identifiers failed to be scraped."
        )
        print(f"{num_scraped / num_filenames:.2%} success rate.")
        print(f"{num_unique:n} unique facility identifiers.")


def scrape_fids(filenames: list[str], fid_regex: re.Pattern) -> list[tuple[str, str]]:
    """
    Returns a list of tuples of scraped FIDs and filenames from the input filename list.

    Arguments:
        inputs(list[str]): A list of filenames.

    Returns:
        list[tuple[str, str]]: A list of tuples of the scraped FIDs and filenames.
    """

    def parse(filename: str) -> str:
        fid = fid_regex.match(filename)
        return fid.group(0) if fid is not None else ""

    return list(zip(map(parse, filenames), filenames))


def args() -> argparse.Namespace:
    """
    Parses the arguments handed to the program.

    Returns:
        Namespace: A namespace of arguments.
    """

    parser = argparse.ArgumentParser(
        description="Scrapes facility identifiers from filenames in the input directories.",
    )
    parser.add_argument(
        "-i",
        "--input",
        action="append",
        dest="input",
        help="The input directories from which to scrape FIDs.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        help="The output directory in which to place a summary CSV.",
    )
    parser.add_argument(
        "-t",
        "--type",
        choices=["sewer", "stormwater"],
        dest="type",
        help="The type of FIDs for which to search.",
        required=True,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        help="Whether to print scraped FIDs to standard output.",
    )
    parser.add_argument(
        "-s",
        "--stats",
        action="count",
        dest="stats",
        help="Whether to print statistics.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main(args())
