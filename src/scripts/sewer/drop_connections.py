import csv
from itertools import chain
from pathlib import Path
from typing import List

import pyodbc

DATABASE_DIR = Path("./databases")
QUERY = """SELECT 
  MH_Inspections.Manhole_Number,
  MH_Connections.Special_Condition
FROM
  MH_Connections
INNER JOIN 
  MH_Inspections ON MH_Connections.InspectionID = MH_Inspections.InspectionID
WHERE
  MH_Connections.Special_Condition LIKE '%Drop%';"""


def access_query(path: Path, query: str) -> List[pyodbc.Row]:
    rows = []

    if path.suffix.lower() in (".mdb", ".accdb"):
        with pyodbc.connect(
            f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path}"
        ) as conn:
            try:
                rows = conn.execute(query).fetchall()
            # raised when tables in query not found, just ignore
            # only correctly shaped databases will have good data anyways
            except pyodbc.ProgrammingError:
                pass

    return rows


rows = (
    # slice off " Lower" and " Upper"
    [row[0], row[1][:-6].title()]
    for row in chain.from_iterable(
        (access_query(db, QUERY) for db in DATABASE_DIR.iterdir())
    )
)

with open("./drop_connections.csv", "w") as file:
    w = csv.writer(file, quoting=csv.QUOTE_ALL, dialect="unix")
    w.writerow(("FACILITYID", "MHTYPE"))
    w.writerows(rows)
