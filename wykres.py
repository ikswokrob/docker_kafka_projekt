import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import json
import altair as alt

# Konfiguracja
st.set_page_config(page_title=" Popularność marek – godzinna ramka", layout="wide")
st_autorefresh(interval=5000, key="auto_refresh")

st.title("Interakcje z markami – ramki godzinne (Live)")

@st.cache_data(ttl=2)
def load_data():
    try:
        with open("stream_log.csv") as f:
            records = [json.loads(line) for line in f.readlines()]
        df = pd.DataFrame(records)
        df['synt_time'] = pd.to_datetime(df['synt_time'])
        return df
    except:
        return pd.DataFrame()

df = load_data()

if not df.empty:
    # Zaokrąglamy czas do pełnej godziny (start okresu)
    df['hour_window'] = df['synt_time'].dt.floor('1H')

    # Znajdź najnowszą godzinę, z której mamy dane
    current_hour = df['hour_window'].max()
    st.info(f"Bieżąca ramka godzinowa: **{current_hour.strftime('%Y-%m-%d %H:%M:%S')}**")

    # Filtrowanie danych tylko z tej jednej godziny
    current_df = df[df['hour_window'] == current_hour]

    # Zlicz interakcje per marka
    brand_counts = current_df['brand'].value_counts().reset_index()
    brand_counts.columns = ['brand', 'interactions']

    st.subheader("Interakcje z markami (w bieżącej godzinie)")

    # Wykres słupkowy
    chart = alt.Chart(brand_counts).mark_bar().encode(
        x=alt.X('brand:N', title="Marka", sort='-y'),
        y=alt.Y('interactions:Q', title="Liczba interakcji"),
        tooltip=['brand', 'interactions']
    ).properties(height=500)

    st.altair_chart(chart, use_container_width=True)

else:
    st.warning("Brak danych...")