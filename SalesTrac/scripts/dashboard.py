import streamlit as st
import pandas as pd
import os
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Superstore Dashboard") \
    .getOrCreate()

# Load dimension and fact tables
fact_sales = spark.read.parquet("/home/daman/Downloads/notebooks/SalesTrac/data/warehouse/fact_sales")
dim_product = spark.read.parquet("/home/daman/Downloads/notebooks/SalesTrac/data/warehouse/dim_product")
dim_customer = spark.read.parquet("/home/daman/Downloads/notebooks/SalesTrac/data/warehouse/dim_customer")
dim_order = spark.read.parquet("/home/daman/Downloads/notebooks/SalesTrac/data/warehouse/dim_order")
dim_location = spark.read.parquet("/home/daman/Downloads/notebooks/SalesTrac/data/warehouse/dim_location")

# Join tables
df = fact_sales \
    .join(dim_product, "product_id") \
    .join(dim_customer, "customer_id") \
    .join(dim_order, "order_id") \
    .join(dim_location, "location_id")

# Convert to Pandas DataFrame
pdf = df.toPandas()
pdf['order_date'] = pd.to_datetime(pdf['order_date'])  # ✅ Convert before using .dt

# Streamlit UI
st.title("📊 Retail Sales Dashboard - Superstore Dataset")

# Sidebar Filters
year_filter = st.sidebar.selectbox("Select Year", sorted(pdf['order_date'].dt.year.unique()))
region_filter = st.sidebar.selectbox("Select Region", sorted(pdf['region'].unique()))

# Filter data
filtered = pdf[
    (pdf['order_date'].dt.year == year_filter) &
    (pdf['region'] == region_filter)
]

# Summary Metrics
st.metric("Total Sales", f"${filtered['sales'].sum():,.2f}")
st.metric("Total Profit", f"${filtered['profit'].sum():,.2f}")
st.metric("Total Orders", filtered['order_id'].nunique())

# Monthly Sales Trend
sales_trend = (
    filtered.groupby(filtered['order_date'].dt.to_period("M"))['sales']
    .sum()
    .reset_index()
)
sales_trend['order_date'] = sales_trend['order_date'].astype(str)
st.line_chart(sales_trend.rename(columns={"order_date": "Month", "sales": "Sales"}).set_index("Month"))

# Top 10 Products by Sales
top_products = (
    filtered.groupby('product_name')['sales']
    .sum()
    .sort_values(ascending=False)
    .head(10)
)
st.bar_chart(top_products)

# Profit by Segment
segment_profit = (
    filtered.groupby('segment')['profit']
    .sum()
    .reset_index()
)
st.bar_chart(segment_profit.set_index("segment"))

df.printSchema()