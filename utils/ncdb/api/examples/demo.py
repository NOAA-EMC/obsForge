import logging
# logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

import os
from datetime import datetime

from ncdb.api import Database


DB_DIR = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/"
DB_PATH = f"{DB_DIR}/cp4.03-parqllel-3dvar.db"

DATA_ROOT = "/scratch4/NCEPDEV/global/John.Steffen/hpss_arch/cp4.03-parallel-3dvar"


def main():
    db = Database(DB_PATH)

    db.scan(DATA_ROOT, -2)

    print("\n=== Datasets ===")
    print(db.list_datasets())

    gdas = db.dataset("gdas")

    print("\n=== Dataset loaded ===\n")

    sst = gdas.obsspace("sst_viirs_n20_l3u")
    print(f"Obs space: {sst.name}\n")

    lon  = sst.variable("longitude")
    lat  = sst.variable("latitude")
    temp = sst.variable("/ObsValue/seaSurfaceTemperature")
    # ice = sst.variable("ombg/seaIceFraction")

    t = datetime(2026, 4, 7, 6)

    print(f"Requesting data at time: {t}\n")

    lon0  = lon[t]
    lat0  = lat[t]
    temp0 = temp[t]

    print("Data loaded:")
    print(f"  lon shape:  {getattr(lon0, 'shape', 'unknown')}")
    print(f"  lat shape:  {getattr(lat0, 'shape', 'unknown')}")
    print(f"  temp shape: {getattr(temp0, 'shape', 'unknown')}")

    # plot(lon0, lat0, temp0)


if __name__ == "__main__":
    main()
