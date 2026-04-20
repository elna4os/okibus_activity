import os
from datetime import datetime, timezone

import pandas as pd
import pydeck as pdk
import streamlit as st

TRIPS_PATH = "data/trips.parquet"
DOW_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
OKINAWA_MAINLAND_CENTER = [26.5, 127.75]
ALL_HOURS = list(range(168))
ALL_HOUR_LABELS = [f"{DOW_LABELS[h // 24]} {h % 24:02d}:00" for h in ALL_HOURS]
START_HOUR = 12


@st.cache_data
def load_trips() -> pd.DataFrame:
    return pd.read_parquet(TRIPS_PATH)


def main():
    # Set up Streamlit page
    st.set_page_config(page_title="Okinawa Buses Activity", layout="wide")
    st.title("🚌 Okinawa Buses Activity")
    st.caption("Project page: https://github.com/elna4os/okibus_activity")
    st.caption("Data source: https://api3.ottop.org")
    st.caption("License: CC BY 4.0")

    # Load data and prepare sidebar options
    df = load_trips()
    mtime = os.path.getmtime(TRIPS_PATH)
    updated_at = datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%d")
    st.sidebar.caption(f"Data updated: {updated_at}")
    sidebar_options = ["All"] + sorted(df["agency_name"].unique())
    selected_operator = st.sidebar.radio("Vendor", options=sidebar_options)
    vendor_df = df if selected_operator == "All" else df[df["agency_name"] == selected_operator]

    # Time slider
    selected_label = st.select_slider(
        "Time",
        options=ALL_HOUR_LABELS,
        value=ALL_HOUR_LABELS[START_HOUR],
    )
    hour_of_week = ALL_HOUR_LABELS.index(selected_label)

    # Filter data for selected hour
    filtered = vendor_df[vendor_df["hour_of_week"] == hour_of_week]

    # Create PyDeck layers
    layers: list[pdk.Layer] = []
    if not filtered.empty:
        agg = (
            filtered
            .groupby("h3_index")["trip_count"]
            .sum()
            .reset_index()
        )

        max_trips = agg["trip_count"].max()
        agg["normalized"] = agg["trip_count"] / max_trips if max_trips > 0 else 0

        layers.append(pdk.Layer(
            "H3HexagonLayer",
            data=agg,
            get_hexagon="h3_index",
            get_fill_color="[255, (1 - normalized) * 255, 0, 180]",
            extruded=False,
            pickable=True,
        ))

    view_state = pdk.ViewState(
        latitude=OKINAWA_MAINLAND_CENTER[0],
        longitude=OKINAWA_MAINLAND_CENTER[1],
        zoom=10,
        pitch=0,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            tooltip={"text": "Trips: {trip_count}"},
            map_style="dark",
        )
    )

    st.metric("Active hexagons", len(agg) if not filtered.empty else 0)


if __name__ == "__main__":
    main()
