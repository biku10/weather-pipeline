import streamlit as st
import pandas as pd
import psycopg2

REDSHIFT_HOST = 'your-redshift-cluster.us-east-1.redshift.amazonaws.com'
REDSHIFT_DB = 'dev'
REDSHIFT_USER = 'your_user'
REDSHIFT_PASSWORD = 'your_password'
REDSHIFT_PORT = 5439
REDSHIFT_TABLE = 'public.weather_data'

def get_data():
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        port=REDSHIFT_PORT
    )
    query = f"""
        SELECT timestamp, city, temp_c, humidity, weather
        FROM {REDSHIFT_TABLE}
        WHERE timestamp > NOW() - INTERVAL '3 days'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="Weather Trends", layout="wide")
st.title("🌤️ Weather Trends Dashboard")

df = get_data()

if df.empty:
    st.warning("No data found.")
else:
    st.sidebar.header("Filter")
    cities = st.sidebar.multiselect("Select cities", sorted(df['city'].unique()), default=df['city'].unique())
    weather_types = st.sidebar.multiselect("Select weather types", sorted(df['weather'].unique()), default=df['weather'].unique())

    filtered_df = df[(df['city'].isin(cities)) & (df['weather'].isin(weather_types))]

    st.subheader("📈 Temperature Over Time")
    st.line_chart(filtered_df.pivot(index='timestamp', columns='city', values='temp_c'))

    st.subheader("💧 Humidity Distribution")
    st.bar_chart(filtered_df.groupby('city')['humidity'].mean())

    st.subheader("🔍 Raw Data")
    st.dataframe(filtered_df.sort_values(by="timestamp", ascending=False).reset_index(drop=True))