# =====================================================================================
# Project 3: Streamlit Dashboard
# =====================================================================================

import streamlit as st
import pandas as pd
import boto3
import requests
import plotly.express as px
import plotly.graph_objects as go

# =====================================================================================
# ⚙️ Configuration
# =====================================================================================

# -----------------------------
# Streamlit Page Layout
# -----------------------------
st.set_page_config(page_title="Project3 Dashboard", layout="wide")
st.title("🍔 Customer & Sales Insights")
st.markdown("---")

dashboard = st.sidebar.selectbox(
    "🔍 Select Dashboard", 
    options=["CI/CD Controls",
             "Overview", 
             "Customer Lifetime Value", 
             "Customer Segmentation",
             "Churn Risk",
             "Sales Trends",
             "Loyalty Orders",
             "Location Performance",
             "Discounts"
             ]
    )

# =====================================================================================
# 1️⃣ CI/CD Controls
# =====================================================================================
REGION = "us-east-1"
PIPELINE_NAME = "project3-github-codepipeline"
GLUE_WORKFLOW_NAME = "project3-bronze-silver-gold"

# -----------------------------
# Load Gold Layer Tables
# -----------------------------
@st.cache_data
def load_data():
    fact_customer = pd.read_parquet("s3://project3-gold-bucket/fact_customer/")
    fact_orders_location = pd.read_parquet("s3://project3-gold-bucket/fact_orders_location/")
    return fact_customer, fact_orders_location

fact_customer, fact_orders_location = load_data()

# -----------------------------
# Trigger CodePipeline
# -----------------------------
def trigger_codepipeline(pipeline_name):
    try:
        client = boto3.client('codepipeline', region_name=REGION)
        response = client.start_pipeline_execution(name=pipeline_name)
        return response['pipelineExecutionId']
    except Exception as e:
        st.error(f"❌ Failed to trigger CodePipeline: {e}")
        return None

# -----------------------------
# Trigger Glue Workflow
# -----------------------------
def trigger_glue_workflow(workflow_name):
    try:
        glue_client = boto3.client('glue', region_name=REGION)
        response = glue_client.start_workflow_run(Name=workflow_name)
        return response['RunId']
    except Exception as e:
        st.error(f"❌ Failed to trigger Glue workflow: {e}")
        return None

# -----------------------------
# Check Dashboard option
# -----------------------------
if dashboard == "CI/CD Controls":
    st.header("⚙️ CI/CD Controls")
    col1, col2, col3 = st.columns(3)
    with col2:
        if st.button("💾 GitHub Deployment via CodePipeline"):
            execution_id = trigger_codepipeline(PIPELINE_NAME)
            if execution_id:
                st.success(f"✅ Pipeline triggered successfully!\nExecution ID: {execution_id}")
    with col3:
        if st.button("⏩ Trigger Glue Job Manually"):
            run_id = trigger_glue_workflow(GLUE_WORKFLOW_NAME)
            if run_id:
                st.success(f"✅ Glue job triggered successfully!\nRun ID: {run_id}")
    st.markdown("---")

# =====================================================================================
# 2️⃣ Overview & Controls
# =====================================================================================
elif dashboard == "Overview":
    st.subheader("Basic Metrics Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Restaurants", fact_orders_location['restaurant_id'].nunique())
    col2.metric("Total Customers", fact_customer['user_id'].nunique())
    col3.metric("Total Orders", fact_customer['orders_daily'].sum())
    col4.metric("Total Revenue ($)", f"{fact_customer['revenue_daily'].sum():,.2f}")
    st.markdown("---")

# =====================================================================================
# 3️⃣ Customer Lifetime Value (CLV) Analysis
# =====================================================================================
elif dashboard == "Customer Lifetime Value":
    st.header("📊 Customer Lifetime Value (CLV) Analysis")

    st.markdown("""
    **Goal:** Estimate how much total revenue a customer generates over their lifetime.
    """)

    # -----------------------------
    # CLV SEGMENTATION & KPIs
    # -----------------------------
    # USER-LEVEL CLV
    user_clv = (
        fact_customer
        .groupby('user_id')['clv_cumulative']
        .max()
    )

    # CLV QUANTILES (20–60–20)
    clv_q20 = user_clv.quantile(0.20)
    clv_q80 = user_clv.quantile(0.80)

    def clv_group(clv):
        if clv <= clv_q20:
            return "Low CLV"
        elif clv >= clv_q80:
            return "High CLV"
        else:
            return "Medium CLV"

    # USER-LEVEL CLV DATAFRAME
    user_clv_df = user_clv.reset_index()
    user_clv_df['CLV_Group'] = user_clv_df['clv_cumulative'].apply(clv_group)

    # MERGE BACK TO FACT TABLE
    fact_customer = fact_customer.merge(
        user_clv_df[['user_id', 'CLV_Group']],
        on='user_id',
        how='left'
    )

    # -----------------------------
    # EXECUTIVE KPIs
    # -----------------------------
    st.subheader("CLV Metrics")

    st.markdown("""
    **Goal:** Estimate how much total revenue a customer will generate over their entire relationship with the business.
    """)

    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Total Lifetime Revenue",
        f"${user_clv.sum():,.2f}"
    )
    col2.metric(
        "Average CLV per Customer",
        f"${user_clv.mean():,.2f}"
    )
    col3.metric(
        "Total High-Value Customers (Top 20%)",
        (user_clv >= clv_q80).sum()
    )
    st.markdown("---")

    # -----------------------------
    # CLV DISTRIBUTION
    # -----------------------------
    st.subheader("CLV Distribution by Segment")

    fig_clv = px.histogram(
        user_clv_df,
        x="clv_cumulative",
        color="CLV_Group",
        nbins=50,
        title="Customer Lifetime Value Distribution",
        labels={'clv_cumulative':'Customer Lifetime Value ($)'},
        category_orders={"CLV_Group": ["High CLV", 
                                       "Medium CLV", 
                                       "Low CLV"
                                       ]
                        },
        color_discrete_map={
            "High CLV": "#ec0c0c",
            "Medium CLV": "#0C008F",
            "Low CLV": "#0a9103"
        }
    )
    fig_clv.update_layout(
        yaxis_title="Customers",
        xaxis_title="Customer Lifetime Value ($)"
    )
    st.plotly_chart(fig_clv, use_container_width=True)
    st.markdown("---")

    # -----------------------------
    # CLV SEGMENT SUMMARY
    # -----------------------------
    st.subheader("CLV Segment Summary")

    clv_summary = (
        user_clv_df
        .groupby("CLV_Group")
        .agg(
            Total_Customers=("user_id", "count"),
            Total_Revenue=("clv_cumulative", "sum"),
            Avg_CLV=("clv_cumulative", "mean")
        )
        .round(2)
        .sort_values(by="Avg_CLV", ascending=False)
        .reset_index()
    )
    st.dataframe(clv_summary, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 4️⃣ Customer Segmentation (RFM)
# =====================================================================================
elif dashboard == "Customer Segmentation":
    st.header("📊 Customer Segmentation (RFM)")

    st.markdown("""
    **Goal:** Group customers based on spending and activity to support campaign targeting.
    """)

    # -----------------------------
    # Helper: Safe qcut
    # -----------------------------
    def safe_qcut(series, q, labels, default_label):
        if series.nunique() <= 1:
            return pd.Series([default_label] * len(series), index=series.index)
        ranked = series.rank(method="first")
        cut = pd.qcut(ranked, q=q, labels=labels, duplicates="drop")
        codes = cut.cat.codes.replace(-1, default_label)
        return codes.astype(int)

    # -----------------------------
    # RFM ANALYSIS
    # -----------------------------
    st.subheader("RFM Customer Segmentation")

    # -----------------------------
    # Window Selection
    # -----------------------------
    window = st.selectbox(
        "Select Time Window",
        options=[7, 30, 90],
        index=1
    )
    freq_col = f"frequency_{window}d"
    mon_col  = f"monetary_{window}d"
    st.markdown("---")

    # -----------------------------
    # Churn Priority Mapping
    # -----------------------------
    churn_priority = {"Low": 1, "Medium": 2, "High": 3}
    reverse_churn = {v: k for k, v in churn_priority.items()}

    # -----------------------------
    # USER-LEVEL AGGREGATION
    # -----------------------------
    rfm_user = (
        fact_customer
        .assign(churn_rank=fact_customer["churn_risk_flag"].map(churn_priority))
        .groupby("user_id", as_index=False)
        .agg(
            Recency=("recency_days", "min"),
            Frequency=(freq_col, "max"),
            Monetary=(mon_col, "max"),
            churn_rank=("churn_rank", "max")
        )
    )
    rfm_user["Churn_Risk"] = rfm_user["churn_rank"].map(reverse_churn)

    # -----------------------------
    # RFM SCORING (1–3)
    # -----------------------------
    rfm_user["R_Score"] = safe_qcut(
        rfm_user["Recency"], q=3, labels=[3,2,1], default_label=3
    )
    rfm_user["F_Score"] = safe_qcut(
        rfm_user["Frequency"], q=3, labels=[1,2,3], default_label=1
    )
    rfm_user["M_Score"] = safe_qcut(
        rfm_user["Monetary"], q=3, labels=[1,2,3], default_label=1
    )
    rfm_user["RFM_Score"] = rfm_user["R_Score"] + rfm_user["F_Score"] + rfm_user["M_Score"]

    # -----------------------------
    # SEGMENT LOGIC
    # -----------------------------
    def rfm_segment(row):
        if row["Churn_Risk"] == "High":
            return "Churn Risk"
        elif row["R_Score"] == 3 and row["F_Score"] >= 2 and row["M_Score"] >= 2:
            return "VIP"
        else:
            return "New Customer"
    rfm_user["Segment"] = rfm_user.apply(rfm_segment, axis=1)

    # -----------------------------
    # KPI METRICS
    # -----------------------------
    st.subheader("📊 RFM KPIs")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Customers", rfm_user.shape[0])
    c2.metric("VIP Customers", (rfm_user["Segment"] == "VIP").sum())
    c3.metric("New Customer", (rfm_user["Segment"] == "New Customer").sum())
    c4.metric("High Churn Risk Customers", (rfm_user["Segment"] == "Churn Risk").sum())
    st.markdown("---")

    # -----------------------------
    # SEGMENT DISTRIBUTION
    # -----------------------------
    st.subheader(f"Customer Segments - Last {window} Days")
    segment_counts = rfm_user.groupby("Segment", as_index=False).agg(Customers=("user_id", "count"))
    segment_colors = {
        "VIP": "#57ff68",
        "New Customer": "#fdff7e",
        "Churn Risk": "#ff5858"
        }
    fig_seg = px.bar(
        segment_counts,
        x="Segment",
        y="Customers",
        color="Segment",
        title=f"RFM Segments ({window}-Day Window)",
        category_orders={"Segment": ["VIP", "New Customer", "Churn Risk"]},
        color_discrete_map=segment_colors
    )
    st.plotly_chart(fig_seg, use_container_width=True)
    st.markdown("---")

    # -----------------------------
    # SUMMARY TABLE
    # -----------------------------
    st.subheader("RFM Segment Summary")
    rfm_summary = rfm_user.groupby("Segment").agg(
        Customers=("user_id", "count"),
        Avg_Recency=("Recency", "mean"),
        Avg_Frequency=("Frequency", "mean"),
        Avg_Monetary=("Monetary", "mean")
    ).round(2).reset_index()
    st.dataframe(rfm_summary, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 5️⃣ Churn Risk Analysis
# =====================================================================================
elif dashboard == "Churn Risk":
    st.header("⚠️ Churn Risk Analysis")

    st.markdown("""
    **Goal:** Build a customer activity profile to help marketing identify at-risk customers.
    """)

    # -----------------------------
    # Get latest churn info per user
    # -----------------------------
    latest_customer = (
        fact_customer.sort_values("creation_date")
                     .groupby("user_id", as_index=False)
                     .last()
    )

    # -----------------------------
    # Churn Risk Counts
    # -----------------------------
    churn_counts = (
        latest_customer['churn_risk_flag']
        .value_counts()
        .reindex(["Low", "Medium", "High"])
        .fillna(0)
        .reset_index()
    )
    churn_counts.columns = ['Churn_Risk', 'Count']

    # -----------------------------
    # Plot with custom colors
    # -----------------------------
    churn_colors = {"Low": "#08c0a5", "Medium": "#ff7f0e", "High": "#d62728"}

    fig_churn = px.bar(
        churn_counts,
        x='Churn_Risk',
        y='Count',
        color='Churn_Risk',
        title="Churn Risk Counts",
        color_discrete_map=churn_colors,
        category_orders={"Churn_Risk": ["Low", "Medium", "High"]}
    )
    st.plotly_chart(fig_churn, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 6️⃣ Sales Trends
# =====================================================================================
elif dashboard == "Sales Trends":
    st.header("📈 Sales Trends and Seasonality")

    st.markdown("""
    **Goal:** Generate time-based summaries to analyze sales patterns.
    """)

    # -----------------------------
    # Total Revenue
    # -----------------------------
    col1, col2, col3 = st.columns([1, 1, 1])  # right column wider
    col3.metric("Total Revenue ($)", f"{fact_customer['revenue_daily'].sum():,.0f}")

    # -----------------------------
    # Daily Revenue
    # -----------------------------
    sales_time = fact_orders_location.groupby('creation_date')['total_revenue'].sum().reset_index()
    fig_line = px.line(sales_time, 
                       x='creation_date', 
                       y='total_revenue', 
                       line_shape="linear",
                       color_discrete_sequence=["#FF8F0E"],
                       labels={'total_revenue':'Revenue','creation_date':'Date'},
                       title="Daily Total Revenue")
    st.plotly_chart(fig_line, use_container_width=True)
    st.markdown("---")

    # -----------------------------
    # Monthly Revenue
    # -----------------------------
    st.subheader("Seasonal Revenue")
    monthly_revenue = fact_orders_location.groupby('period_month')['total_revenue'].sum().reset_index()
    fig_month = px.bar(monthly_revenue, 
                       x='period_month', 
                       y='total_revenue', 
                       color_discrete_sequence=["#F900C3"],
                       labels={'period_month':'Month','total_revenue':'Revenue'},
                       title="Revenue by Month")
    st.plotly_chart(fig_month, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 7️⃣ Loyalty Program Impact
# =====================================================================================
elif dashboard == "Loyalty Orders":
    st.header("🎖️ Loyalty Orders")

    st.markdown("""
    **Goal:** Compare loyalty members vs non-members in terms of spend and engagement.
    """)

    # -----------------------------
    # Create flag for loyalty
    # -----------------------------
    fact_customer['is_loyalty'] = fact_customer['loyalty_orders'] > 0

    # -----------------------------
    # Average spend per loyalty flag
    # -----------------------------
    avg_spend = fact_customer.groupby('is_loyalty', as_index=False)['revenue_daily'].mean()

    fig_loyalty = px.bar(
        avg_spend, 
        x='is_loyalty', 
        y='revenue_daily', 
        color='is_loyalty',
        color_discrete_map={True: "#32FF0E", False: "#9500FF"},
        hover_name='is_loyalty',
        hover_data={'revenue_daily': ':.2f'},
        labels={'is_loyalty':'Loyalty Orders', 'revenue_daily':'Average Daily Revenue'},
        title="Average Spend: Loyalty vs Non-Loyalty"
    )
    st.plotly_chart(fig_loyalty, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 8️⃣ Location Performance
# =====================================================================================
elif dashboard == "Location Performance":
    st.header("🏣 Top 5 Performing Locations")

    st.markdown("""
    **Goal:** Identify best and worst-performing store locations.
    """)

    # -----------------------------
    # Top 5 Locations by Revenue
    # -----------------------------
    loc_rev = fact_orders_location.groupby('restaurant_id')['total_revenue'].sum().sort_values(ascending=False).reset_index()
    fig_loc_rev = px.bar(loc_rev.head(5), 
                         x='restaurant_id', 
                         y='total_revenue', 
                         color_discrete_sequence=["#6C3907"],
                         labels={'restaurant_id':'Restaurants', 'total_revenue':'Total Revenue'},
                         title="Top 5 Locations by Revenue")
    st.plotly_chart(fig_loc_rev, use_container_width=True)
    st.markdown("---")

    # -----------------------------
    # Top 5 Average Order Value by Location
    # -----------------------------
    st.subheader("Average Order Value by Location")
    loc_aov = fact_orders_location.groupby('restaurant_id')['avg_order_value'].mean().sort_values(ascending=False).reset_index()
    fig_loc_aov = px.bar(loc_aov.head(5), 
                         x='restaurant_id', 
                         y='avg_order_value', 
                         color_discrete_sequence=["#007135"],
                         labels={'restaurant_id':'Restaurants', 'avg_order_value':'Avg Order Value ($)'},
                         title="Top 5 Locations by Avg Order Value")
    st.plotly_chart(fig_loc_aov, use_container_width=True)
    st.markdown("---")

# =====================================================================================
# 9️⃣ Pricing & Discounts
# =====================================================================================
elif dashboard == "Discounts":
    st.header("💲Pricing & Discount Effectiveness")

    st.markdown("""
    **Goal:** Measure how discounts affect revenue and profitability.
    """)

    fact_orders_location['discount_applied'] = fact_orders_location['non_loyalty_revenue'] < fact_orders_location['total_revenue']
    discount_rev = fact_orders_location.groupby('discount_applied')['total_revenue'].sum().reset_index()
    fig_discount = px.bar(discount_rev, 
                          x='discount_applied', 
                          y='total_revenue', 
                          color='discount_applied',
                          color_discrete_map={True: "#2F00FF", False: "#DE1F06"},
                          labels={'discount_applied':'Discount Applied', 'total_revenue':'Total Revenue'}, 
                          title="Revenue: Discount vs Non-Discount")
    st.plotly_chart(fig_discount, use_container_width=True)
    st.markdown("---")
