import streamlit as st
import pandas as pd
import numpy as np


@st.cache_data
def load_data():
    df = pd.read_csv("data/hdb-property-info.csv")
    print(len(df))
    return df


st.title("HDB Information")

df = load_data()

st.dataframe(df)
