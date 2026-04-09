import argparse
import os
import uvicorn
# from app.db import init_db
# from app.viewer import app

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="app server")
    parser.add_argument(
        "--port",
        type=int,
        default=8734,
        help="server listens on this port",
    )   
    parser.add_argument("--db", required=True, help="Path to the SQLite database")
    args = parser.parse_args()

    abs_db_path = os.path.abspath(args.db)
    # init_db(abs_db_path)
    # print(f"Starting server using database: {abs_db_path}")

    os.environ["NCDB_DB_PATH"] = abs_db_path
    
    print(f"Manager: Starting server on port {args.port}")
    print(f"Manager: Targeting DB: {abs_db_path}")

    # app.state.db_path = os.path.abspath(args.db)

    uvicorn.run(
        "app.viewer:app",
        host="0.0.0.0",
        port=args.port,
        reload=True,
        log_level="info"
    )
