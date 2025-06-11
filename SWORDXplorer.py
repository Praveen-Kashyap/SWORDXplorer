import streamlit as st
import requests
import pandas as pd
import geopandas as gpd
from io import StringIO
import time
import os
import shutil
import tempfile
from datetime import datetime

# --- Helper Functions ---
def robust_rmtree(path):
    """Robustly remove directory tree with retry logic"""
    for _ in range(3):
        try:
            shutil.rmtree(path)
            return True
        except Exception:
            time.sleep(0.5)
    return False


def safe_temp_dir():
    """Create a secure temporary directory"""
    temp_dir = tempfile.mkdtemp(prefix="sword_temp_")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def get_valid_api_fields():
    """Return list of valid API fields"""
    return [
        "area_det_u", "area_detct", "area_total", "area_tot_u", "area_wse",
        "collection_shortname", "collection_version", "continent_id", "crid", "cycle_id",
        "dark_frac", "d_x_area", "d_x_area_u",
        "dschg_b", "dschg_b_q", "dschg_bsf", "dschg_b_u",
        "dschg_c", "dschg_c_q", "dschg_csf", "dschg_c_u",
        "dschg_gb", "dschg_gb_q", "dschg_gbsf", "dschg_gb_u",
        "dschg_gc", "dschg_gc_q", "dschg_gcsf", "dschg_gc_u",
        "dschg_gh", "dschg_gh_q", "dschg_ghsf", "dschg_gh_u",
        "dschg_gi", "dschg_gi_q", "dschg_gisf", "dschg_gi_u",
        "dschg_gm", "dschg_gm_q", "dschg_gmsf", "dschg_gm_u",
        "dschg_go", "dschg_go_q", "dschg_gosf", "dschg_go_u",
        "dschg_gq_b", "dschg_s", "dschg_s_q", "dschg_ssf", "dschg_s_u",
        "dschg_h", "dschg_h_q", "dschg_hsf", "dschg_h_u",
        "dschg_i", "dschg_i_q", "dschg_isf", "dschg_i_u",
        "dschg_m", "dschg_m_q", "dschg_msf", "dschg_m_u",
        "dschg_o", "dschg_o_q", "dschg_osf", "dschg_o_u",
        "dschg_q_b", "dry_trop_c", "geometry", "geoid_hght", "geoid_slop",
        "granuleUR", "ice_clim_f", "ice_dyn_f", "ingest_time", "iono_c",
        "layovr_val", "loc_offset", "load_tidef", "load_tideg", "n_good_nod",
        "n_reach_dn", "n_reach_up", "node_dist", "obs_frac_n", "p_dam_id",
        "p_dist_out", "p_lat", "p_length", "p_lon", "p_low_slp", "p_maf",
        "p_n_ch_max", "p_n_ch_mod", "p_n_nodes", "p_wid_var", "p_width",
        "p_wse", "p_wse_var", "partial_f", "pass_id", "pole_tide",
        "range_end_time", "range_start_time", "reach_id", "reach_q", "reach_q_b",
        "rch_id_dn", "rch_id_up", "river_name", "slope", "slope2", "slope2_r_u",
        "slope2_u", "slope_r_u", "slope_u", "solid_tide", "sword_version",
        "time", "time_str", "time_tai", "wse", "wse_c", "wse_c_u",
        "wse_r_u", "wse_u", "width", "width_c", "width_c_u", "width_u",
        "xovr_cal_c", "xovr_cal_q", "xtrk_dist"
    ]

# --- App configuration ---
st.set_page_config(page_title="SWORDXplorer", layout="wide")
st.title("SWOT Hydrocron API Data Download")

# --- Session State Initialization ---
if 'geo_df' not in st.session_state:
    st.session_state.geo_df = None
if 'upload_key' not in st.session_state:
    st.session_state.upload_key = None
if 'valid_fields' not in st.session_state:
    st.session_state.valid_fields = get_valid_api_fields()

# --- Main Layout ---
st.header("Data Input")

# SWORD file upload
sword_file = st.file_uploader(
    "Upload SWORD Shapefile", 
    type=['shp', 'dbf', 'shx', 'prj'],
    accept_multiple_files=True,
    key="sword_uploader"
)

# Process uploaded files
if sword_file:
    current_key = tuple((f.name, f.size) for f in sword_file)
    if current_key != st.session_state.upload_key:
        temp_dir = safe_temp_dir()
        for file in sword_file:
            with open(os.path.join(temp_dir, file.name), 'wb') as out:
                out.write(file.getbuffer())
        shp_files = [f for f in os.listdir(temp_dir) if f.endswith('.shp')]
        if shp_files:
            try:
                df = gpd.read_file(os.path.join(temp_dir, shp_files[0]))
                st.session_state.geo_df = df
                st.success(f"Loaded {len(df)} river reaches")
                st.session_state.upload_key = current_key
            except Exception as e:
                st.error(f"Error loading shapefile: {e}")
        else:
            st.error("No .shp file found in upload")
        robust_rmtree(temp_dir)

# Show filters if data loaded
if st.session_state.geo_df is not None:
    st.subheader("Filter Parameters")
    cols = st.session_state.geo_df.columns.tolist()
    filter_column = st.selectbox("Filter By Column", options=cols, index=0)
    distinct_vals = st.session_state.geo_df[filter_column].dropna().unique().tolist()
    if distinct_vals:
        filter_value = st.selectbox("Filter Value", options=distinct_vals)
    else:
        st.warning("No values in selected column")
        filter_value = None

    st.subheader("Time Range")
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Start Date", datetime(2023, 8, 1))
    with c2:
        end_date = st.date_input("End Date", datetime(2025, 5, 30))

    st.subheader("API Fields to Retrieve")
    valid_fields = st.session_state.valid_fields
    fc, bc = st.columns([3, 1])
    with fc:
        selected_fields = st.multiselect(
            "Select fields to retrieve from API",
            valid_fields,
            default=st.session_state.get("selected_fields", ["reach_id", "time_str", "wse", "width"]),
            key="api_fields_widget"
        )
        st.session_state.selected_fields = selected_fields
    with bc:
        if st.button("Select All Fields"):
            st.session_state.selected_fields = valid_fields

    throttle_delay = st.slider("Request Delay (seconds)", 0.1, 2.0, 0.5, 0.1)

    if st.button("🚀 Process Data", type="primary") and filter_value and st.session_state.selected_fields:
        df_geo = st.session_state.geo_df
        filtered = df_geo[df_geo[filter_column] == filter_value]
        reach_ids = filtered['reach_id'].dropna().astype(str).tolist()
        if not reach_ids:
            st.warning(f"No reach IDs for {filter_value}")
            st.stop()
        st.info(f"Processing {len(reach_ids)} reaches for {filter_value}")

        safe_val = "".join(c for c in filter_value if c.isalnum() or c in " _-")
        OUTPUT_DIR = f"swot_{safe_val}_output"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        LOG_FILE = os.path.join(OUTPUT_DIR, "logger.txt")

        results = []
        errors = 0
        pbar = st.progress(0)
        status = st.empty()

        for i, rid in enumerate(reach_ids):
            pbar.progress((i+1)/len(reach_ids))
            status.info(f"Fetching reach {rid} ({i+1}/{len(reach_ids)})...")
            url = (
                f"https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?"
                f"feature=Reach&feature_id={rid}"
                f"&start_time={start_date}T00:00:00Z"
                f"&end_time={end_date}T00:00:00Z"
                f"&output=csv&fields={','.join(st.session_state.selected_fields)}"
            )
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 429:
                    retry = int(resp.headers.get('Retry-After', 10))
                    status.warning(f"Rate limited, retrying in {retry}s...")
                    time.sleep(retry)
                    resp = requests.get(url, timeout=30)
                if resp.status_code == 400:
                    st.warning(f"Bad request for reach {rid}: {resp.text}")
                    # Log not-found reaches
                    with open(LOG_FILE, "a") as logf:
                        logf.write(f"{datetime.now().isoformat()} - {rid}\n")
                    errors += 1
                    continue
                resp.raise_for_status()
                data = resp.json()
                csv_txt = data.get('results', {}).get('csv', '')
                if csv_txt:
                    df = pd.read_csv(StringIO(csv_txt))
                    if 'time_str' in df.columns:
                        df['time_str'] = pd.to_datetime(df['time_str'], errors='coerce')
                        df = df.dropna(subset=['time_str'])
                    df.to_csv(os.path.join(OUTPUT_DIR, f"reach_{rid}.csv"), index=False)
                    results.append(df)
                else:
                    st.warning(f"No data for reach {rid}")
                    errors += 1
            except Exception as e:
                st.error(f"Error with reach {rid}: {e}")
                errors += 1
            finally:
                time.sleep(throttle_delay)

        if results:
            combined = pd.concat(results, ignore_index=True)
            combined_path = os.path.join(OUTPUT_DIR, f"combined_{safe_val}.csv")
            combined.to_csv(combined_path, index=False)
            st.success(f"Processed {len(results)} reaches with {errors} errors")
            with st.expander("Combined Data"):
                st.dataframe(combined)
            with open(combined_path, "rb") as f:
                st.download_button("💾 Download CSV", f, file_name=os.path.basename(combined_path))
            st.info(f"All files saved in {OUTPUT_DIR}")
        else:
            st.warning("No data retrieved.")

if st.session_state.geo_df is None:
    st.info("Upload a SWORD shapefile to start.")
elif not st.session_state.get('selected_fields'):
    st.info("Select API fields and click Process Data.")
