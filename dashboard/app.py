"""Healthcare claims dashboard.

Read-only by design: every query hits `healthcare.marts` and nothing else. dbt
builds and tests those tables; this app's job is to display them.

Run with:  .venv/bin/streamlit run dashboard/app.py
Env:       set -a; source .env; set +a   (same SNOWFLAKE_* vars dbt uses)
"""

import os
from decimal import Decimal

import pandas as pd
import snowflake.connector
import streamlit as st

DATABASE = "healthcare"
SCHEMA = "marts"

# Payments, not charges: charges are what was billed, payments are what was
# actually paid -- the only figure traceable to claim lines.
HEADLINE = """
select
    (select sum(total_paid)      from agg_claims_by_provider) as total_paid,
    (select sum(claim_count)     from agg_claims_by_provider) as claims,
    (select count(*)             from dim_patients)          as patients,
    (select count(*)             from agg_cost_per_procedure) as procedure_codes
"""

# agg_claims_by_provider is provider x month, so "top providers" re-aggregates.
TOP_PROVIDERS = """
select
    coalesce(provider_name, provider_id) as provider,
    sum(total_paid)                      as paid
from agg_claims_by_provider
group by 1
order by paid desc
limit 10
"""

# Codes charged but not paid have a null total_paid; excluded so the chart shows
# spend rather than sorting nulls to the top (Snowflake puts NULL first in DESC).
TOP_PROCEDURES = """
select
    procedure_code,
    total_paid,
    total_charge,
    payment_count
from agg_cost_per_procedure
where total_paid is not null
order by total_paid desc
limit 15
"""

UTILIZATION = """
select
    encounter_month,
    encounter_class,
    sum(encounter_count) as encounter_count
from agg_utilization_trends
group by 1, 2
order by 1
"""

AGE_BANDS = """
select age_band, count(*) as patients
from dim_patients
group by 1
order by 1
"""

GENDER = """
select gender, count(*) as patients
from dim_patients
group by 1
order by 2 desc
"""


@st.cache_resource
def get_connection() -> snowflake.connector.SnowflakeConnection:
    """One connection for the life of the server process.

    cache_resource, not cache_data: a connection cannot be hashed or copied, and
    sharing the object itself is the point.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=DATABASE,
        schema=SCHEMA,
    )


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    """Run a query and return a DataFrame with lowercase column names.

    10-minute cache: Streamlit reruns the whole script on every interaction, and
    every uncached query resumes the warehouse for a 60-second minimum.

    Snowflake folds unquoted identifiers to uppercase, so names are lowered here
    instead of quoted everywhere. Decimals become floats so the charts can read
    them; string columns are left alone, because a procedure code must stay a
    string ("03" is not 3).
    """
    with get_connection().cursor() as cursor:
        cursor.execute(sql)
        columns = [column[0].lower() for column in cursor.description]
        frame = pd.DataFrame(cursor.fetchall(), columns=columns)

    for column in frame.columns:
        values = frame[column].dropna()
        if not values.empty and isinstance(values.iloc[0], Decimal):
            frame[column] = frame[column].astype(float)

    return frame


def millions(value: float) -> str:
    return f"${value / 1_000_000:,.1f}M"


st.set_page_config(page_title="Healthcare claims", layout="wide")
st.title("Healthcare claims")
st.caption("Synthetic Synthea data · Airflow + Spark → S3 → Snowflake → dbt marts")

headline = run_query(HEADLINE).iloc[0]
metrics = st.columns(4)
metrics[0].metric("Total paid", millions(headline["total_paid"]))
metrics[1].metric("Claims", f"{int(headline['claims']):,}")
metrics[2].metric("Patients", f"{int(headline['patients']):,}")
metrics[3].metric("Procedure codes", f"{int(headline['procedure_codes']):,}")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Top providers by paid amount")
    providers = run_query(TOP_PROVIDERS)
    # Names are long and the chart is half-width; shorten rather than let
    # Streamlit ellipsise mid-word ("Annabelle933 Sta...").
    labels = providers.set_index("provider")["paid"]
    labels.index = [name if len(name) <= 24 else name[:23] + "…" for name in labels.index]
    st.bar_chart(labels, horizontal=True, height=420)
    st.caption("Payments only — charges are what was billed, not what was paid.")

with right:
    st.subheader("Top procedures by paid amount")
    procedures = run_query(TOP_PROCEDURES)
    st.bar_chart(procedures.set_index("procedure_code")["total_paid"], height=420)
    st.caption("Codes charged but not yet paid are excluded (null total_paid).")

st.subheader("Utilization by month and encounter class")
utilization = run_query(UTILIZATION)
# Synthea's history starts in 1928 and is flat until the last decade, so the raw
# range compresses everything interesting into the right edge. Windowed here in
# pandas rather than in SQL: the query is cached, a second query would not be.
utilization["encounter_month"] = pd.to_datetime(utilization["encounter_month"])
window_start = utilization["encounter_month"].max() - pd.DateOffset(years=10)
utilization = utilization[utilization["encounter_month"] >= window_start]
utilization_wide = utilization.pivot(
    index="encounter_month", columns="encounter_class", values="encounter_count"
).fillna(0)
st.line_chart(utilization_wide)
st.caption(
    "Last 10 years of a history that starts in 1928 — the earlier decades are "
    "flat and would squash the recent trend into a sliver."
)

demographics_left, demographics_right = st.columns(2)

with demographics_left:
    st.subheader("Patients by age band")
    st.bar_chart(run_query(AGE_BANDS).set_index("age_band")["patients"])

with demographics_right:
    st.subheader("Patients by gender")
    st.bar_chart(run_query(GENDER).set_index("gender")["patients"])

st.divider()
st.caption(
    "Cost basis: **payments** from claim lines — the only figure traceable to "
    "claims. Charges exceed payments by design. Every table is read from "
    "`healthcare.marts`; this app never writes."
)
