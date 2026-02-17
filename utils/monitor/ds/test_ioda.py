# from sqlalchemy import create_session
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .db_base import Base
from .ioda_structure import IodaStructure




def test_ioda(db_path):
    # 1. Setup Session
    # session = create_session(bind=engine)

    # db_path = "jemcda"
    # Create engine & session
    engine = create_engine(f"sqlite:///{db_path}")
    # engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)
    session = Session(engine)


    test_file = "/lfs/h2/emc/da/noscrub/emc.da/obsForge/COMROOT/realtime/gfs.20260202/12/ocean/sst/gfs.t12z.sst_viirs_npp_l3u.nc"

    try:
        # First Pass (Creation)
        struct_id_1 = IodaStructure.get_or_create_id(test_file, session)
        print(f"Pass 1 ID: {struct_id_1}")

        # Second Pass (Retrieval)
        struct_id_2 = IodaStructure.get_or_create_id(test_file, session)
        print(f"Pass 2 ID: {struct_id_2}")

        assert struct_id_1 == struct_id_2
        print("Success: Structural hashing and retrieval working correctly.")

    except Exception as e:
        session.rollback()
        print(f"Test failed: {e}")
    finally:
        session.close()
