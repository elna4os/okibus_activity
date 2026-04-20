# Okinawa Buses Activity

Interactive heatmap showing bus activity across mainland Okinawa, broken down by operator and hour of the week.

Built with Streamlit and PyDeck.  
Data source: [OTTOP API](https://api3.ottop.org) (CC BY 4.0).

## How it works

1. **Download** — GTFS feeds for mainland Okinawa bus operators are fetched from the OTTOP API
2. **Prepare** — stop-times are aggregated into H3 hexagons (resolution 8) and hourly bins (168 hours/week), saved as a Parquet file
3. **Visualize** — Streamlit app reads the Parquet file and renders an H3 heatmap with a time slider and operator filter

## Install dependencies

```bash
pip install -r requirements.txt --no-cache-dir
```

## Usage

```bash
# 1. Download GTFS data
python scripts/download_gtfs.py

# 2. Prepare data
python scripts/prepare_data.py

# 3. Run the app
streamlit run app.py
```
