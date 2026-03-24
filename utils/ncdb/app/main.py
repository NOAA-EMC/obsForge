import os
from datetime import datetime

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter

from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Session

from db import session

from ds.dataset_orm import DatasetORM, DatasetCycleORM, DatasetFieldORM, DatasetFileORM
from ds.netcdf_structure_orm import NetcdfNodeORM

from ds.dataset import Dataset
from ds.dataset_field import DatasetField
from ds.dataset_cycle import DatasetCycle
from ds.obs_space import ObsSpace
from ds.netcdf_structure import NetcdfStructure

from plotting.plot_generator import PlotGenerator
# from products_server import DataProductsServer


app = FastAPI()
templates = Jinja2Templates(directory="templates")

# app.mount("/static", StaticFiles(directory="static"), name="static")

# data_products_dir = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data_products"
# server = DataProductsServer(data_products_dir)

# Absolute path on the filesystem
# Update this to your preferred scratch location
BASE_DATA_PRODUCTS_DIR = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data_products/viewer"

# Ensure the directory exists
os.makedirs(BASE_DATA_PRODUCTS_DIR, exist_ok=True)

# Map the URL prefix "/products" to the physical scratch folder
app.mount("/products", StaticFiles(directory=BASE_DATA_PRODUCTS_DIR), name="products")


# ----------------------------------
# Pages
# ----------------------------------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    datasets = session.query(DatasetORM).all()
    return templates.TemplateResponse("index.html", {"request": request, "datasets": datasets})


# ----------------------------------
# API Endpoints
# ----------------------------------
@app.get("/datasets/{dataset_id}/fields")
def get_fields(dataset_id: int):
    dataset_orm = session.get(DatasetORM, dataset_id)
    if not dataset_orm:
        return JSONResponse({"error": "Dataset not found"}, status_code=404)

    # the non-recursive from_orm
    ds = Dataset.from_db_self(dataset_orm) 
    ds.from_orm_fields(session)

    # Return the data from the domain objects
    return [
        {"id": field.id, "name": field.obs_space.name} 
        for field in ds.dataset_fields
    ]


@app.get("/fields/{field_id}/cycles")
def get_cycles(field_id: int):
    # print(f"\n[DEBUG] Fetching cycles for field_id: {field_id}")

    # 1. Fetch the Field ORM to get the Dataset link
    f_orm = session.get(DatasetFieldORM, field_id)
    if not f_orm:
        print(f"[DEBUG] ERROR: Field {field_id} not found in DB")
        return []
    
    # print(f"[DEBUG] Found Field: {f_orm.obs_space.name} (Dataset ID: {f_orm.dataset_id})")

    # 2. Initialize the Dataset Domain Object
    # We use f_orm.dataset which is the relationship link to DatasetORM
    ds = Dataset.from_db_self(f_orm.dataset)
    # print(f"[DEBUG] Dataset Domain Object initialized: {ds.name} (ID: {ds.id})")

    # 3. Load the Cycle Axis
    # This is where we see if the DB has cycles for this dataset
    ds.load_cycles_from_db(session)
    # print(f"[DEBUG] Cycles loaded from DB: {len(ds.dataset_cycles)} found")

    # 4. Optional: Print the first few cycles to verify date/hour formats
    # if ds.dataset_cycles:
        # first = ds.dataset_cycles[0]
        # print(f"[DEBUG] Sample Cycle: {first.cycle_date} {first.cycle_hour}")

    results = [
        {"date": c.cycle_date.isoformat(), "hour": c.cycle_hour} 
        for c in ds.dataset_cycles
    ]
    
    # print(f"[DEBUG] Returning {len(results)} cycles to the frontend\n")
    return results


@app.get("/fields/{field_id}/variables")
def get_variables(field_id: int):
    field_orm = session.get(DatasetFieldORM, field_id)
    if not field_orm:
        return []
    field = DatasetField.from_orm_self(field_orm, dataset=None) 
    return field.obs_space.netcdf_structure.list_variables()

def generate_history_plot(session, field, variable, plotter):
    df = field.get_variable_derived_data(session, variable)
    if df.empty:
        logger.debug(f"No history found for {variable}")
        return None

    # 2. Pathing
    fname = f"{field.obs_space.name}_history.png"
    plot_path = os.path.join(plotter.output_dir, fname)

    plotter.generate_history_plot_pd(
        df=df,
        val_col="mean",      # Matches the metric name in your DB/DataFrame
        std_col="stddev",    # Matches the metric name in your DB/DataFrame
        title=f"History: {variable}",
        y_label="Value",
        out_path=plot_path
    )

    return fname

def old_generate_history_plot(session, field, variable, plotter):
    """
    Standalone Function: Orchestrates the vertical slice (Time Series).
    """
    # 1. Get the Pivoted DataFrame
    # Columns are: 'mean', 'stddev' (or whatever metrics are in DB)
    # Index is: 'ts' (the timestamp)
    df = field.get_variable_derived_data(session, variable)

    if df.empty:
        print(f"[DEBUG] No history found for {variable}")
        return None

    # 2. Convert to plotter format
    # Since 'ts' is the INDEX, we use row.Index
    plot_ready_data = []
    for row in df.itertuples(index=True):
        try:
            # Check if required metrics exist in this row
            val = getattr(row, 'mean', None)
            std = getattr(row, 'stddev', None)
            
            if val is None:
                continue

            plot_ready_data.append({
                "date": row.Index.strftime("%Y%m%d"),
                "cycle": row.Index.hour,
                "mean": val,
                "stddev": std
            })
        except Exception as e:
            print(f"[DEBUG] Row error: {e}")
            continue

    if not plot_ready_data:
        return None

    # 3. Pathing and Plotting
    fname = f"{field.obs_space.name}_history.png"
    plot_path = os.path.join(plotter.output_dir, fname)

    plotter.generate_history_plot_with_moving_avg(
        plot_path=plot_path,
        data=plot_ready_data,
        title=f"History: {variable}",
        val_key="mean",
        std_key="stddev",
        y_label="Value"
    )

    return fname


@app.post("/generate-plot")
def generate_plot(
    dataset_id: int = Form(...),
    field_id: int = Form(...),
    cycle_date: str = Form(...),
    cycle_hour: str = Form(...),
    variable: str = Form(...),
    plot_type: str = Form(...),
):
    # --- 1. Basic Identity Setup ---
    ds_orm = session.get(DatasetORM, dataset_id)
    f_orm = session.get(DatasetFieldORM, field_id)
    
    if not ds_orm or not f_orm:
        return JSONResponse({"error": "Dataset or Field not found"}, status_code=404)

    # Reconstruct the Dataset skeleton
    ds = Dataset.from_db_self(ds_orm)


    # Use the ABSOLUTE path for writing to disk
    # This creates: /scratch3/.../data_products/viewer/gdas/2026-03-04_00/
    output_dir = os.path.join(BASE_DATA_PRODUCTS_DIR, ds.name, f"{cycle_date}_{cycle_hour}")
    os.makedirs(output_dir, exist_ok=True)
    plotter = PlotGenerator(output_dir)


    
    # Prepare output directory
    # output_dir = os.path.join("products", ds.name, f"{cycle_date}_{cycle_hour}")
    # os.makedirs(output_dir, exist_ok=True)
    # plotter = PlotGenerator(output_dir)

    # --- 2. Branching by Plot Type (The Data Slices) ---
    
    if plot_type == "historical":
        field = DatasetField.from_orm(session, f_orm, ds)
        fname = generate_history_plot(session, field, variable, plotter)
        
        if not fname:
            return JSONResponse({"error": "No data found for plot"}, status_code=404)

        url = f"/products/{ds.name}/{cycle_date}_{cycle_hour}/{fname}"
        return JSONResponse({"url": url})

        '''
        # Hydrate the Field with its history 
        # This uses DatasetField.from_orm logic we wrote earlier
        # field = DatasetField.from_orm(session, f_orm, ds, n_files=50)
        field = DatasetField.from_orm(session, f_orm, ds)
        
        # Build the DataFrame using the logic we moved into the Field class
        df = field.get_variable_derived_data(session, variable)
        
        if df.empty:
            return JSONResponse({"error": "No historical stats found in DB"}, status_code=404)

        # Plot using the DataFrame
        fname = f"{field.obs_space.name}_history.png"
        plotter.generate_history_plot_from_df(df, variable, fname)
        url = f"/products/{ds.name}/{cycle_date}_{cycle_hour}/{fname}"
        '''

    elif plot_type in ["surface", "interactive"]:
        target_date = datetime.strptime(cycle_date, "%Y-%m-%d").date()
        # cycle_domain = ds.read_cycle_from_db(session, target_date, cycle_hour)
        cycle_domain = DatasetCycle.from_db(session, ds, target_date, cycle_hour)

        if not cycle_domain:
            return JSONResponse({"error": "Cycle data not found"}, status_code=404)

        ds_file = next((f for f in cycle_domain.files if f.dataset_field.id == field_id), None)
        
        if not ds_file:
            return JSONResponse({"error": "Field data missing for this cycle"}, status_code=404)

        # Extract spatial data (lats, lons, values)
        try:
            data = ds_file.get_surface_variable_data(variable)
            plot_payload = {
                "dataset_name": ds.name,
                "obs_space_name": ds_file.dataset_field.obs_space.name,
                "variable_name": variable,
                **data # Spreads lats, lons, values, units
            }
        except Exception as e:
            return JSONResponse({"error": f"NetCDF Read Error: {str(e)}"}, status_code=500)

        # Dispatch to correct plotter method
        if plot_type == "surface":

            # Generate the file
            fname = f"{ds_file.dataset_field.obs_space.name}_surface.png"
            plotter.generate_surface_map(os.path.join(output_dir, fname), plot_payload)

        else: # interactive
            fname = f"int_{variable.replace('/', '_')}.html"
            plotter.generate_interactive_surface_map(os.path.join(output_dir, fname), plot_payload)
            
        url = f"/products/{ds.name}/{cycle_date}_{cycle_hour}/{fname}"

    else:
        return JSONResponse({"error": f"Unsupported plot type: {plot_type}"}, status_code=400)

    return JSONResponse({"url": url})
