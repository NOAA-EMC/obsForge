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

    obsspace_names = gdas.list_obsspaces()
    # print(f"{obsspace_names}\n")
    print(f"{len(obsspace_names)} obs spaces:")
    for name in sorted(obsspace_names):
        print(f"- {name}")

    sst = gdas.obsspace("sst_viirs_n20_l3u")
    print(f"Obs space: {sst.name}\n")

    variable_names = sst.list_variables()
    # print(f"{variable_names}\n")
    print(f"{len(variable_names)} for {sst.name}:\n")
    for name in sorted(variable_names):
        print(f"- {name}")

    lon  = sst.field("longitude")
    lat  = sst.field("latitude")
    temp = sst.field("/ObsValue/seaSurfaceTemperature")
    # ice = sst.field("ombg/seaIceFraction")

    t = datetime(2026, 4, 7, 6)

    print(f"Requesting data at time: {t}\n")

    # lon0  = lon[t]
    # lat0  = lat[t]
    temp0 = temp[t]

    print("Data loaded:")
    # print(f"  lon shape:  {getattr(lon0.data, 'shape', 'unknown')}")
    # print(f"  lat shape:  {getattr(lat0.data, 'shape', 'unknown')}")
    print(f"  temp0 shape: {getattr(temp0.data, 'shape', 'unknown')}")
    lon0 = temp0.coords['longitude']
    lat0 = temp0.coords['latitude']
    print(f"  temp0 coordinates: {getattr(lon0, 'shape', 'unknown')}")
    print(f"  temp0 coordinates: {getattr(lat0, 'shape', 'unknown')}")

    plot_path = temp0.plot("jtemp0.png")
    print(f"Plot generated at {plot_path}")

    temp_max = temp.max
    tmax = temp_max[t]
    temp_min = temp.min
    tmin = temp_min[t]

    print(f"max temp at {t} = {tmax}")
    print(f"200 + min temp at {t} = {tmin + 200.0}")

    plot_path = temp_max.plot("jtemp_max.png")
    print(f"History plot generated at {plot_path}")

'''
    fields 
        lazy-evaluation;
        hold no data; encode computation graph
        are DB aware
        may trigger DB or netcdf read
    values
        in memory data

    # aggregation:
    temp = gdas.field("ObsValue/seaSurfaceTemperature", obsspaces="sst_*")

    # temporal selection may be useful for plotting, etc
    # historic plot:
    temp.between(t1, t2)
    max_temp = temp.max
    max_temp.plot("temp.png")

    # value at a given time:
    # triggers evaluation!
    # the field encodes the union; evaluation involves
    # a db query
    temp0 = temp[t]
    temp0.plot("temp0.png")

    temp1 = temp0.subset(lat=(0, 30), lon=(-80, -20))
    temp2 = temp1.where(temp1 > 300)

    # algebra:
    dt = temp0 - temp00
    max_dt = dt.max
'''


if __name__ == "__main__":
    main()
