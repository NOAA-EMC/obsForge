import argparse
import uvicorn


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="app server")
    parser.add_argument(
        "--port",
        type=int,
        default=8734,
        help="server listens on this port",
    )   
    args = parser.parse_args()
    if args.port:
        port = args.port
    else:
        port = 8734 # likely redundant

    # run the server
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    )
