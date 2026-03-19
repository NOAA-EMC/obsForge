from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from db import session  # your SQLAlchemy session
from ds.dataset_orm import DatasetORM, DatasetCycleORM, DatasetFieldORM, DatasetFileORM
from plotting.plot_generator import PlotGenerator
from products_server import DataProductsServer

# from ds.dataset import DatasetField
from ds.obs_space import ObsSpace
from ds.netcdf_structure import NetcdfStructure

from sqlalchemy.orm import joinedload

from fastapi import APIRouter
from sqlalchemy.orm import Session
from ds.netcdf_structure_orm import NetcdfNodeORM

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# app.mount("/static", StaticFiles(directory="static"), name="static")

data_products_dir = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data_products"
server = DataProductsServer(data_products_dir)


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
    fields = session.query(DatasetFieldORM).filter(DatasetFieldORM.dataset_id == dataset_id).all()
    return [{"id": f.id, "name": f.obs_space.name} for f in fields]


@app.get("/fields/{field_id}/cycles")
def get_cycles(field_id: int):
    files = session.query(DatasetCycleORM).join(DatasetFileORM).filter(DatasetFileORM.dataset_field_id == field_id).all()
    cycles = sorted({(c.cycle_date.isoformat(), c.cycle_hour) for c in files})
    return [{"date": d, "hour": h} for d, h in cycles]


'''
@app.get("/fields/{field_id}/variables")
def old_get_variables(field_id: int):
    field = session.get(DatasetFieldORM, field_id)

    structure = field.obs_space.netcdf_structure

    variables = [
        node.full_path
        for node in structure.nodes
        if node.node_type == "variable"
    ]

    print(f"field: {field}")
    print(f"obs_space: {field.obs_space}")
    print(f"structure: {field.obs_space.netcdf_structure}")
    print(f"variables: {field.obs_space.netcdf_structure.list_variables('/ombg')}")

    return variables
'''

@app.get("/fields/{field_id}/variables")
def get_variables(field_id: int):
    # fetch the field ORM
    field: DatasetFieldORM = session.get(DatasetFieldORM, field_id)
    if not field:
        return []

    # get the structure ORM from obs_space
    structure = field.obs_space.netcdf_structure
    if not structure:
        return []

    # query all VARIABLE nodes under '/ombg'
    variables = (
        session.query(NetcdfNodeORM.full_path)
        .filter(
            NetcdfNodeORM.structure_id == structure.id,
            NetcdfNodeORM.node_type == "VARIABLE" #,
            # NetcdfNodeORM.full_path.like("/ombg/%")  # limit to your desired group
        )
        .order_by(NetcdfNodeORM.full_path)
        .all()
    )

    # flatten from list of tuples
    return [v[0] for v in variables]



@app.post("/generate-plot")
def generate_plot(
    dataset_id: int = Form(...),
    field_id: int = Form(...),
    cycle_date: str = Form(...),
    cycle_hour: str = Form(...),
    variable: str = Form(...),
    plot_type: str = Form(...),
):

    dataset = session.get(DatasetORM, dataset_id)
    field = session.get(DatasetFieldORM, field_id)
    
    # Load DatasetField and its relationships (including ObsSpaceORM and NetcdfStructureORM)
    field_with_relationships = session.query(DatasetFieldORM).filter(DatasetFieldORM.id == field_id).options(
        joinedload(DatasetFieldORM.obs_space).joinedload(ObsSpaceORM.netcdf_structure).joinedload(NetcdfStructureORM.nodes)
    ).first()
    
    # Access the related ObsSpaceORM and NetcdfStructureORM
    obs_space = field_with_relationships.obs_space
    netcdf_structure = obs_space.netcdf_structure
    


    # --- 1. Fetch ORM objects ---
    '''
    dataset = session.get(DatasetORM, dataset_id)
    # field = session.get(DatasetFieldORM, field_id)

	# Modify this query to eagerly load 'obs_space', 'netcdf_structure', and 'netcdf_structure.nodes'
    field = session.query(DatasetFieldORM).options(
        joinedload(DatasetFieldORM.obs_space).joinedload(ObsSpace.netcdf_structure).joinedload(NetcdfStructure.nodes)
	).get(field_id)
    '''


    if not dataset or not field:
        return JSONResponse({"error": "Dataset or Field not found"}, status_code=404)

    cycle = session.query(DatasetCycleORM).filter(
        DatasetCycleORM.dataset_id == dataset_id,
        DatasetCycleORM.cycle_date == cycle_date,
        DatasetCycleORM.cycle_hour == cycle_hour
    ).first()
    if not cycle:
        return JSONResponse({"error": "Cycle not found"}, status_code=404)

    file = field.dataset_files[0]  # Pick the first file for simplicity

    # --- 2. Initialize the DatasetFile object ---
    try:
        # Initialize DatasetFile to ensure that the associated NetcdfFile is set up correctly
        dataset_file = DatasetFile(file=file.file, dataset_field=field, dataset_cycle=cycle)
    except Exception as e:
        return JSONResponse({"error": f"Failed to initialize DatasetFile: {str(e)}"}, status_code=500)

    # --- 3. Prepare plot output directory ---
    output_dir = os.path.join("products", dataset.name, f"{cycle_date}_{cycle_hour}")
    os.makedirs(output_dir, exist_ok=True)

    plotter = PlotGenerator(output_dir)

    # --- 4. Handle plot types ---
    url = None

    if plot_type == "historical":
        # --- Build "dirty" historical data list ---
        data = []
        for c in field.obs_space.list_cycles():  # Your existing ORM method or dummy
            val = field.obs_space.get_variable_value(variable, c)
            if val is None:
                continue
            data.append({
                "date": c.cycle_date,     # YYYYMMDD
                "cycle": int(c.cycle_hour),  # hour as int
                variable: val
            })

        if not data:
            return JSONResponse({"error": "No data for variable"}, status_code=404)

        fname = f"{field.obs_space.name}.png"
        plotter.generate_history_plot(
            title=f"{variable} History",
            data=data,
            val_key=variable,
            std_key=None,   # Or pass your std key if available
            fname=fname,
            y_label="Value",
        )
        url = f"/products/{dataset.name}/{cycle_date}_{cycle_hour}/{fname}"

    elif plot_type == "surface":
        # --- Fetch lat/lon/values directly from the DatasetFile ---
        try:
            variable_path = variable
            surface_data = dataset_file.get_surface_variable_data(variable_path)
            lats = surface_data["lats"]
            lons = surface_data["lons"]
            values = surface_data["values"]
            units = surface_data["units"]
        except Exception as e:
            return JSONResponse({"error": f"Error fetching surface data: {str(e)}"}, status_code=500)

        # Prepare plot data for surface plot
        plot_data = {
            "dataset_name": dataset.name,
            "obs_space_name": field.obs_space.name,
            "lats": lats,
            "lons": lons,
            "values": values,
            "variable_name": variable,
            "units": units  # Units extracted from the NetCDF file
        }

        # Generate the surface plot
        fname = f"{field.obs_space.name}_surface.png"
        plotter.generate_surface_map(os.path.join(output_dir, fname), plot_data)

        # Prepare the URL
        url = f"/products/{dataset.name}/{cycle_date}_{cycle_hour}/{fname}"

    elif plot_type == "interactive":
        try:
            # Same for interactive plot as for surface plot
            variable_path = f"/{variable}"  # Adjust this if needed
            surface_data = dataset_file.get_surface_variable_data(variable_path)
            lats = surface_data["lats"]
            lons = surface_data["lons"]
            values = surface_data["values"]
            units = surface_data["units"]
        except Exception as e:
            return JSONResponse({"error": f"Error fetching surface data: {str(e)}"}, status_code=500)

        plot_data = {
            "dataset_name": dataset.name,
            "obs_space_name": field.obs_space.name,
            "lats": lats,
            "lons": lons,
            "values": values,
            "variable_name": variable,
            "units": units
        }

        fname = f"int_{variable}.html"
        plotter.generate_interactive_surface_map(os.path.join(output_dir, fname), plot_data)

        url = f"/products/{dataset.name}/{cycle_date}_{cycle_hour}/{fname}"

    else:
        return JSONResponse({"error": f"Unknown plot_type: {plot_type}"}, status_code=400)

    return JSONResponse({"url": url})
