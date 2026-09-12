import pandas as pd
import streamlit as st
from app import store

st.set_page_config(page_title="Job Queue Observatory", layout="wide")
store.initialize()
st.title("Distributed Job Queue Observatory")
st.caption("Refresh the page to view the latest SQLite-backed scheduler state.")
data, counts = store.dashboard_data(), store.dashboard_data()["counts"]
cols = st.columns(5)
for col, label, key in zip(cols, ["Queued", "Running", "Succeeded", "DLQ", "Total"], ["queued", "running", "succeeded", "dead_letter", "total"]):
    col.metric(label, sum(counts.values()) if key == "total" else counts.get(key, 0))
left, right = st.columns(2)
with left:
    st.subheader("Worker heartbeats"); st.dataframe(pd.DataFrame(data["workers"]), use_container_width=True)
with right:
    st.subheader("Dead Letter Queue"); st.dataframe(pd.DataFrame(data["dead_letters"]), use_container_width=True)
st.subheader("Recent scheduler events")
st.dataframe(pd.DataFrame(data["events"]), use_container_width=True)
