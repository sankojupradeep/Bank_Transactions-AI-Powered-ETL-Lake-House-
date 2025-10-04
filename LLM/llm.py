import streamlit as st
import pandas as pd
import plotly.express as px
from groq import Groq
import json
import tempfile
from weasyprint import HTML
from streamlit_plotly_events import plotly_events
import boto3
from io import StringIO, BytesIO
from datetime import datetime
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *

# ---------------- S3/MinIO Config ----------------
S3_ENDPOINT = "http://localhost:9002"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password123"
BUCKET = "bank-project"

s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# ---------------- Groq API Setup ----------------
client = Groq(api_key="gsk_juEeRPwBNz0tanikV8tsWGdy...........................")

# ---------------- Spark Setup for Gold Delta Read (Fixed Delta Extension) ----------------
# Set Delta JAR at top-level for proper loading
delta_jar = "/mnt/c/Users/Hello/delta-spark_2.13-3.2.0.jar"  # Updated to 3.3.0 for Spark 4.0/Scala 2.13
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--jars {delta_jar} pyspark-shell"

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Gold-Dashboard") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.local.ip", "127.0.0.1") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true")
    return builder.getOrCreate()
# ---------------- S3 Functions for Gold Fact Tables ----------------
def read_gold_fact_table(table_name):
    """Read Gold fact Delta table from S3 as DataFrame."""
    gold_path = f"s3a://{BUCKET}/gold/{table_name}"
    try:
        spark = get_spark_session()
        df = spark.read.format("delta").load(gold_path)
        df_pd = df.toPandas()
        spark.stop()
        return df_pd
    except Exception as e:
        st.error(f"Error reading Gold fact table {table_name}: {e}")
        return pd.DataFrame()

# ---------------- LLM Functions ----------------
def get_dashboard_spec(prompt, df_facts):
    """Call LLaMA 3 via Groq API to get dashboard JSON spec based on Gold fact data."""
    system_msg = """
    You are a data assistant. 
    Return JSON with:
    {
      "charts": [
        {"id":"chart1","type":"line","title":"Monthly Spend Trends","sql":"SELECT month, total_spend FROM df_facts"},
        {"id":"chart2","type":"bar","title":"Top Customers by Spend","sql":"SELECT customer_id, SUM(amount) as spend FROM df_facts GROUP BY customer_id ORDER BY spend DESC LIMIT 10"},
        {"id":"chart3","type":"pie","title":"Spend by Category","sql":"SELECT category, SUM(amount) as spend FROM df_facts GROUP BY category"}
      ],
      "summary": "Brief analysis for decision-making (e.g., top trends, insights).",
      "recommendations": "Business recommendations from data (e.g., target high-spend customers)."
    }
    Use the provided df_facts DataFrame for queries. Focus on decision-making insights like trends, top performers. Limit to 50k rows.
    """
    # Prepare data summary for LLM
    facts_summary = df_facts.to_json(orient='records')[:1000]  # Truncate for prompt
    full_prompt = f"{prompt}\nGold Fact Data: {facts_summary}"

    response = client.chat.completions.create(
        model="llama3-70b-8192",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": full_prompt}
        ],
        temperature=0.2,
    )
    return json.loads(response.choices[0].message.content)

# ---------------- Dashboard Functions ----------------
def run_sql_on_df(df, sql):
    """Run SQL on DataFrame (for LLM-generated queries)."""
    try:
        return df.query(sql) if 'query' in sql.lower() else df  # Simple pd.query; use spark.sql for complex
    except Exception as e:
        st.error(f"SQL error: {e}")
        return pd.DataFrame()

def safe_append_filter(sql_query, condition):
    """Safely append WHERE or AND to SQL."""
    if "where" in sql_query.lower():
        return sql_query + " AND " + condition
    else:
        return sql_query + " WHERE " + condition

def plot_chart(df, chart_type, title):
    """Return Plotly figure for a chart type."""
    if df.empty:
        st.warning(f"No data available for {title}")
        return None

    if chart_type == "bar":
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=title)
    elif chart_type == "line":
        fig = px.line(df, x=df.columns[0], y=df.columns[1], title=title)
    elif chart_type == "pie":
        fig = px.pie(df, names=df.columns[0], values=df.columns[1], title=title)
    else:
        st.warning(f"Chart type {chart_type} not supported")
        return None
    fig.update_layout(clickmode='event+select')  
    return fig

# ---------------- Streamlit UI ----------------
st.set_page_config(layout="wide")
st.markdown("<h1> AI-Powered Interactive Dashboard for Gold Fact Tables</h1>", unsafe_allow_html=True)

# Default message (so page is never blank)
st.info("Select a fact table and enter a query to generate decision-making dashboards.")

# Select fact table
fact_table = st.selectbox("Select Gold Fact Table:", ["fact_transaction_summary", "fact_customer_spend"])  # Adjust based on your tables

user_prompt = st.text_input("Enter your dashboard query (e.g., 'show monthly spend trends and top customers for decision-making'):")

# Filters
st.markdown("<h2> Filters</h2>", unsafe_allow_html=True)
col1, col2 = st.columns(2)
with col1:
    date_filter = st.date_input("Filter Date Range:", value=datetime.now())
with col2:
    category_filter = st.text_input("Filter Category (optional):", "")

# ---------------- Generate Dashboard ----------------
if st.button("Generate Dashboard") and user_prompt.strip():
    try:
        # Read Gold fact table from S3
        df_facts = read_gold_fact_table(fact_table)
        if df_facts.empty:
            st.warning("No data in selected fact table. Check S3 Gold path.")
            st.stop()

        spec = get_dashboard_spec(user_prompt, df_facts)

        chart_dataframes = {}
        chart_placeholders = {}

        # Layout 2 charts per row
        columns = st.columns(2)

        for idx, chart in enumerate(spec["charts"]):
            sql_query = chart["sql"]

            # Apply filters safely (on S3 data)
            if category_filter:
                sql_query = safe_append_filter(sql_query, f"category LIKE '%{category_filter}%'")

            df = run_sql_on_df(df_facts, sql_query)
            chart_dataframes[chart["id"]] = df

            col = columns[idx % 2]
            with col:
                fig = plot_chart(df, chart["type"], chart["title"])
                placeholder = st.empty()
                chart_placeholders[chart["id"]] = placeholder

                if fig:
                    # Capture click events
                    clicked_points = plotly_events(fig, click_event=True)
                    if clicked_points:
                        st.session_state.clicked_value = str(clicked_points[0]['x'])

                    placeholder.plotly_chart(fig, use_container_width=True)

        # ---------------- Drill-down / Cross-filter ----------------
        st.markdown("<h2>🖱 Drill-down / Cross-filter</h2>", unsafe_allow_html=True)
        st.write("Click on any chart element to filter all charts dynamically.")

        # Input box as fallback
        filter_value = st.text_input("Or enter value to filter charts manually:")
        if filter_value:
            st.session_state.clicked_value = filter_value

        # Update charts if clicked_value is set
        if "clicked_value" in st.session_state and st.session_state.clicked_value:
            clicked_value = st.session_state.clicked_value
            st.write(f"Filtering all charts by: {clicked_value}")
            for chart_id, df in chart_dataframes.items():
                if not df.empty:
                    # Simple filter on first column
                    filtered_df = df[df.iloc[:, 0].astype(str) == clicked_value]
                    fig = plot_chart(filtered_df, spec["charts"][0]["type"], f"{spec['charts'][0]['title']} (Filtered)")
                    if fig:
                        chart_placeholders[chart_id].plotly_chart(fig, use_container_width=True)

        # ---------------- LLM Summary & Recommendations ----------------
        st.markdown("<h2> LLM Analysis & Decision-Making Insights</h2>", unsafe_allow_html=True)
        if df_facts.shape[0] > 0:
            analysis = spec.get("summary", "No analysis available.")
            recommendations = spec.get("recommendations", "No recommendations available.")
            st.write("**Analysis:**", analysis)
            st.write("**Recommendations:**", recommendations)

        # ---------------- Export ----------------
        st.markdown("<h2> Export Dashboard</h2>", unsafe_allow_html=True)

        if st.button("Export as HTML"):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
                # Export all charts into one HTML file
                html_content = "<h1>Gold Fact Dashboard Export</h1>"
                for df in chart_dataframes.values():
                    if not df.empty:
                        fig = plot_chart(df, "bar", "Exported Chart")
                        if fig:
                            html_content += fig.to_html(full_html=False, include_plotlyjs="cdn")
                tmp.write(html_content.encode())
                tmp.flush()
                st.success(f"Dashboard exported as {tmp.name}")

        if st.button("Export as PDF"):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as html_tmp:
                html_content = "<h1>Dashboard Export</h1>"
                for df in chart_dataframes.values():
                    if not df.empty:
                        fig = plot_chart(df, "bar", "Exported Chart")
                        if fig:
                            html_content += fig.to_html(full_html=False, include_plotlyjs="cdn")
                html_tmp.write(html_content.encode())
                html_tmp.flush()
                pdf_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
                HTML(html_tmp.name).write_pdf(pdf_file.name)
                st.success(f"Dashboard exported as {pdf_file.name}")

    except Exception as e:
        st.error(f"Error: {e}")