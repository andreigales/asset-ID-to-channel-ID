import streamlit as st
import pandas as pd
from pandas.errors import ParserError
import zipfile
import io
import os

# =========================
# Settings (stability)
# =========================
MAX_CSV_FILES = 12
CHUNK_SIZE = 200_000  # reduce (e.g. 50_000) if you still hit RAM issues

# =========================
# Helper: robust chunk reader
# =========================
def iter_chunks_with_fallback(file_like, header=None, skiprows=0, chunksize=CHUNK_SIZE):
    """
    Iterates CSV chunks robustly.
    - First try: engine='c', sep=',' with on_bad_lines='skip'
    - If parsing fails (including mid-iteration): restart and fallback to engine='python', sep=None (auto-detect)
    """
    def make_reader(engine, sep):
        try:
            file_like.seek(0)
        except Exception:
            pass
        return pd.read_csv(
            file_like,
            header=header,
            skiprows=skiprows,
            dtype=str,
            low_memory=False,
            chunksize=chunksize,
            engine=engine,
            sep=sep,
            on_bad_lines="skip",
        )

    # First attempt
    try:
        reader = make_reader(engine="c", sep=",")
        for chunk in reader:
            yield chunk
        return
    except ParserError:
        pass
    except Exception:
        # For any other issue, fallback as well
        pass

    # Fallback attempt (restart)
    reader2 = make_reader(engine="python", sep=None)
    for chunk in reader2:
        yield chunk


# =================
# Business Helpers
# =================
def load_channel_data(excel_file):
    """
    Load channel IDs and names from uploaded Excel file.
    Expected format:
    - Column A (index 0): Channel IDs
    - Column B (index 1): Channel Names
    
    Returns a dictionary: {channel_id: channel_name}
    """
    try:
        df = pd.read_excel(excel_file, header=None, dtype=str)
        
        if df.shape[1] < 2:
            st.error("Excel file must have at least 2 columns: Channel ID and Channel Name")
            return {}
        
        # Clean channel IDs
        channel_ids = (
            df.iloc[:, 0]
            .astype(str)
            .str.replace("\xa0", " ")
            .str.strip()
        )
        
        # Clean channel names
        channel_names = (
            df.iloc[:, 1]
            .astype(str)
            .str.replace("\xa0", " ")
            .str.strip()
        )
        
        # Create dictionary mapping channel_id -> channel_name
        channel_dict = {}
        for cid, cname in zip(channel_ids, channel_names):
            if cid and cid.lower() != "nan" and cname and cname.lower() != "nan":
                channel_dict[cid] = cname
        
        return channel_dict
        
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return {}


def process_csv_for_channel_assets(file_like, display_name, channel_dict):
    """
    Process CSV to find Asset IDs that correspond to Channel IDs.
    
    - Column D (index 3): Asset ID
    - Column G (index 6): Channel ID
    
    Returns DataFrame with columns: Asset ID, Channel ID, Channel Name
    """
    ASSET_COL_IDX = 3   # Column D (0-indexed)
    CHANNEL_COL_IDX = 6  # Column G (0-indexed)
    
    channel_ids_set = set(channel_dict.keys())
    results = []
    
    try:
        for chunk in iter_chunks_with_fallback(file_like, header=None, skiprows=0):
            # Check if chunk has enough columns
            if chunk.shape[1] <= max(ASSET_COL_IDX, CHANNEL_COL_IDX):
                continue
            
            # Clean the channel ID column
            chunk[CHANNEL_COL_IDX] = (
                chunk[CHANNEL_COL_IDX]
                .astype(str)
                .str.replace("\xa0", " ")
                .str.strip()
            )
            
            # Clean the asset ID column
            chunk[ASSET_COL_IDX] = (
                chunk[ASSET_COL_IDX]
                .astype(str)
                .str.replace("\xa0", " ")
                .str.strip()
            )
            
            # Filter rows where channel ID is in our list
            filtered = chunk[chunk[CHANNEL_COL_IDX].isin(channel_ids_set)]
            
            if filtered.empty:
                continue
            
            # Extract Asset ID and Channel ID
            subset = filtered[[ASSET_COL_IDX, CHANNEL_COL_IDX]].copy()
            subset.columns = ["Asset ID", "Channel ID"]
            
            # Add Channel Name by mapping from our dictionary
            subset["Channel Name"] = subset["Channel ID"].map(channel_dict)
            
            results.append(subset)
            
    except Exception as e:
        st.warning(f"Error reading {display_name}: {e}")
        return None
    
    if not results:
        return None
    
    return pd.concat(results, ignore_index=True)


# =========================
# ZIP handling
# =========================
def extract_csv_filelikes_from_zip(zip_bytes: bytes):
    """
    Returns list of tuples: (display_name, file_like)
    where file_like is a BytesIO ready for pandas.
    """
    csv_items = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for info in z.infolist():
            # Skip folders / hidden files
            if info.is_dir():
                continue
            name = info.filename
            base = os.path.basename(name)
            if not base:
                continue
            if base.startswith("__MACOSX") or "/__MACOSX" in name:
                continue
            if base.startswith("."):
                continue
            if base.lower().endswith(".csv"):
                data = z.read(info)
                csv_items.append((base, io.BytesIO(data)))
    return csv_items


# ==========
# Main UI
# ==========
st.set_page_config(page_title="Channel to Asset Mapper", layout="wide")
st.title("🔗 Channel to Asset Mapper")

st.markdown("""
This app maps YouTube Channel IDs to their corresponding Asset IDs.

**How it works:**
1. Upload an Excel file with Channel IDs (column A) and Channel Names (column B)
2. Upload the raw CSV report files (or a ZIP containing them)
3. The app finds all Asset IDs associated with those channels

**Output:** A table with Asset ID, Channel ID, and Channel Name
""")

st.subheader("1. Upload Channel List")
uploaded_excel = st.file_uploader(
    "Upload Excel file with Channel IDs and Names (Column A: Channel ID, Column B: Channel Name)",
    type=["xlsx", "xls"]
)

st.subheader("2. Upload Raw Data Files")
st.markdown("Choose one of the following options:")

uploaded_csvs = st.file_uploader(
    "Option A: Upload CSV Files (up to 12)",
    type=["csv"],
    accept_multiple_files=True,
    key="csv_multi",
)

uploaded_zip = st.file_uploader(
    "Option B: Upload a ZIP containing CSV files",
    type=["zip"],
    accept_multiple_files=False,
    key="zip_single",
)

# Enforce 12 max for multi-CSV selection
if uploaded_csvs and len(uploaded_csvs) > MAX_CSV_FILES:
    st.error(f"Maximum {MAX_CSV_FILES} CSV files allowed. You selected {len(uploaded_csvs)}.")
    st.stop()

# Prepare inputs list: (display_name, file_like)
input_csv_items = []

# If ZIP provided, use ZIP
if uploaded_zip is not None:
    try:
        zip_bytes = uploaded_zip.getvalue()
        extracted = extract_csv_filelikes_from_zip(zip_bytes)
        if not extracted:
            st.error("The ZIP file contains no .csv files.")
            st.stop()
        if len(extracted) > MAX_CSV_FILES:
            st.error(f"ZIP contains {len(extracted)} CSVs. Maximum allowed: {MAX_CSV_FILES}.")
            st.stop()
        input_csv_items = extracted
        st.info(f"Found {len(input_csv_items)} CSV files in ZIP.")
    except Exception as e:
        st.error(f"Could not read ZIP file: {e}")
        st.stop()

# Else use direct CSV uploads
elif uploaded_csvs:
    input_csv_items = [(f.name, f) for f in uploaded_csvs]

# Generate
if st.button("🚀 Generate Mapping"):
    if not uploaded_excel:
        st.error("Please upload the Channel list Excel file first.")
        st.stop()
    
    if not input_csv_items:
        st.error("Please upload CSV files or a ZIP containing CSV files.")
        st.stop()
    
    with st.spinner("Processing files..."):
        # Load channel data
        channel_dict = load_channel_data(uploaded_excel)
        st.success(f"Loaded {len(channel_dict)} channel IDs with names.")
        
        if not channel_dict:
            st.error("No valid channel data found in the Excel file.")
            st.stop()
        
        # Show preview of loaded channels
        with st.expander("Preview loaded channels"):
            preview_df = pd.DataFrame([
                {"Channel ID": k, "Channel Name": v} 
                for k, v in list(channel_dict.items())[:10]
            ])
            st.dataframe(preview_df)
            if len(channel_dict) > 10:
                st.caption(f"... and {len(channel_dict) - 10} more")
        
        all_results = []
        progress_bar = st.progress(0)
        
        for i, (display_name, file_like) in enumerate(input_csv_items):
            # Ensure pointer at start for each file
            try:
                file_like.seek(0)
            except Exception:
                pass
            
            res = process_csv_for_channel_assets(file_like, display_name, channel_dict)
            
            if res is not None and not res.empty:
                all_results.append(res)
                st.write(f"✅ Found {len(res)} matches in {display_name}")
            else:
                st.write(f"⚪ No matches in {display_name}")
            
            progress_bar.progress((i + 1) / len(input_csv_items))
        
        if not all_results:
            st.warning("No matching channel IDs found in any of the uploaded files.")
            st.stop()
        
        # Combine all results
        final_df = pd.concat(all_results, ignore_index=True)
        
        # Remove duplicates (same asset might appear in multiple files)
        final_df = final_df.drop_duplicates(subset=["Asset ID", "Channel ID"])
        
        # Sort by Channel Name, then Asset ID
        final_df = final_df.sort_values(["Channel Name", "Asset ID"]).reset_index(drop=True)
        
        st.success(f"### Found {len(final_df)} unique Asset-Channel mappings")
        
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Mappings", len(final_df))
        with col2:
            st.metric("Unique Assets", final_df["Asset ID"].nunique())
        with col3:
            st.metric("Unique Channels", final_df["Channel ID"].nunique())
        
        # Show the results
        st.dataframe(final_df, use_container_width=True)
        
        # Download button
        csv_bytes = final_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 Download Mapping CSV",
            data=csv_bytes,
            file_name="channel_asset_mapping.csv",
            mime="text/csv",
        )
