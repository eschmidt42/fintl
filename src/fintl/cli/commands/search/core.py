"""
CLI tool to interactively search through bank transactions.

Example data:

┌────────┬──────────┬────────┬──────────┬─────────┬─────────┬─────────┬────────┬─────────┬─────────┐
│ source ┆ recipien ┆ amount ┆ descript ┆ date    ┆ provide ┆ service ┆ parser ┆ file    ┆ hash    │
│ ---    ┆ t        ┆ ---    ┆ ion      ┆ ---     ┆ r       ┆ ---     ┆ ---    ┆ ---     ┆ ---     │
│ str    ┆ ---      ┆ f64    ┆ ---      ┆ date    ┆ ---     ┆ str     ┆ str    ┆ str     ┆ u64     │
│        ┆ str      ┆        ┆ str      ┆         ┆ str     ┆         ┆        ┆         ┆         │
╞════════╪══════════╪════════╪══════════╪═════════╪═════════╪═════════╪════════╪═════════╪═════════╡
│ myself ┆ EXAMPLE  ┆ -100.0 ┆ 2022-10- ┆ 2022-10 ┆ DKB     ┆ giro    ┆ giro0  ┆ /home/a ┆ 1234567 │
│        ┆ BANK     ┆        ┆ 12       ┆ -14     ┆         ┆         ┆        ┆ lice/Do ┆ 8901234 │
│        ┆ MAIN ST  ┆        ┆ Debitk.0 ┆         ┆         ┆         ┆        ┆ cuments ┆ 56789   │
│        ┆          ┆        ┆ 0 VISA…  ┆         ┆         ┆         ┆        ┆ /Paperw ┆         │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ ork…    ┆         │
│ myself ┆ Santa    ┆ -131.0 ┆ Foobar   ┆ 2022-05 ┆ DKB     ┆ giro    ┆ giro0  ┆ /home/a ┆ 9876543 │
│        ┆ Clause   ┆        ┆          ┆ -09     ┆         ┆         ┆        ┆ lice/Do ┆ 2109876 │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ cuments ┆ 54321   │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ /Paperw ┆         │
│        ┆          ┆        ┆          ┆         ┆         ┆         ┆        ┆ ork…    ┆         │
└────────┴──────────┴────────┴──────────┴─────────┴─────────┴─────────┴────────┴─────────┴─────────┘
"""

from fintl.cli.commands.search.tui import TableApp


def run():
    app = TableApp()
    app.run()
