"""Churn Risk Checker — a Databricks App.

Flow: pick a customer -> look up RFM features (SQL Statement API)
      -> call the Model Serving endpoint -> show churn risk in plain English.
"""
import os
import re

import requests
from flask import Flask, render_template_string, request
from databricks.sdk import WorkspaceClient

app = Flask(__name__)

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")

ENDPOINT_NAME = "retail-churn-classifier"
FEATURE_TABLE = "retail_ai_prod.features.customer_rfm_features"
WAREHOUSE_ID = os.environ.get("CHURN_WAREHOUSE_ID", "72ea3b2da9f7464d")

FEATURE_COLUMNS = [
    "frequency", "monetary", "avg_order_value", "total_units_purchased",
    "total_returns", "return_rate", "category_diversity",
    "customer_tenure_days", "loyalty_score", "channel_score",
]

PAGE = """
<!doctype html>
<html>
<head>
  <title>Churn Risk Checker</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 620px; margin: 40px auto; padding: 0 16px; }
    h1 { color: #FF3621; }
    select, button { font-size: 16px; padding: 8px; margin-right: 8px; }
    .result { margin-top: 24px; padding: 20px; border-radius: 10px; font-size: 18px; }
    .high { background: #fdecea; border-left: 6px solid #ff3621; }
    .low  { background: #e8f5e9; border-left: 6px solid #2e7d32; }
  .gauge { margin-top: 16px; background: #eee; border-radius: 6px; height: 22px; overflow: hidden; }
  .gauge-fill { height: 100%; border-radius: 6px; }
  .fill-high { background: #ff3621; }
  .fill-low  { background: #2e7d32; }
  .stats { display: flex; gap: 32px; margin-top: 16px; }
  .stat small { color: #777; }
  .action { margin-top: 14px; font-weight: bold; }
  </style>
</head>
<body>
  <h1>Customer Churn Risk Checker</h1>
  <p>Pick a customer — the app looks up their RFM features and asks the
     <b>{{ endpoint }}</b> serving endpoint for the churn probability.</p>

  <form method="post" action="/predict">
    <select name="customer_id">
      {% for cid in customers %}<option value="{{ cid }}">{{ cid }}</option>{% endfor %}
    </select>
    <button type="submit">Check churn risk</button>
  </form>

  {% if result %}
  <div class="result {{ result.css }}">
    <b>{{ result.customer_id }}</b> — churn risk <b>{{ result.risk_pct }}%</b><br>
    {{ result.verdict }}
    <div class="gauge"><div class="gauge-fill {{ 'fill-high' if result.high else 'fill-low' }}" style="width: {{ result.risk_pct }}%"></div></div>
    <div class="stats">
      <div><b>{{ result.recency_days | int }}</b><br><small>days since last purchase</small></div>
      <div><b>{{ result.frequency | int }}</b><br><small>orders in the period</small></div>
      <div><b>₹{{ result.monetary | int }}</b><br><small>total spend</small></div>
    </div>
    <p class="action">{{ result.action }}</p>
  </div>
  {% endif %}

  <p style="color:#888;margin-top:32px">
    Model: <code>retail_ai_prod.models.customer_churn_serving</code> via
    <code>{{ endpoint }}</code>
  </p>
</body>
</html>
"""


def run_sql(sql: str, parameters: list = None):
    """Run one query on the SQL warehouse (Spark is not available inside apps)."""
    response = w.statement_execution.execute_statement(
        statement=sql,
        parameters=parameters,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="30s",
    )
    if response.status.error:
        raise RuntimeError(response.status.error.message)
    return response.result.data_array


def fetch_customers(limit: int = 50):
    rows = run_sql(
        f"SELECT customer_id FROM {FEATURE_TABLE} "
        f"ORDER BY customer_id LIMIT {limit}"
    )
    return [row[0] for row in rows]


def fetch_customer(customer_id: str):
    """Look up the 10 model features + the headline numbers for one customer."""
    if not re.fullmatch(r"C\d{4}", customer_id):
        raise ValueError("Invalid customer id")
    cols = ", ".join(FEATURE_COLUMNS + ["recency_days"])
    # customer_id is strictly validated (C####) above, so inlining it is safe —
    # dict-style "parameters" are rejected by databricks-sdk 0.67+
    rows = run_sql(
        f"SELECT {cols} FROM {FEATURE_TABLE} WHERE customer_id = '{customer_id}'"
    )
    if not rows:
        raise ValueError(f"Customer {customer_id} not found")
    values = [float(v) for v in rows[0]]
    return {
        "features": values[:-1],        # the 10 columns the model expects
        "recency_days": values[-1],     # headline numbers for the result card
        "frequency": values[0],
        "monetary": values[1],
    }


def predict_churn(features: list) -> float:
    """Call the Model Serving endpoint — same call as any external app."""
    response = requests.post(
        f"{HOST}/serving-endpoints/{ENDPOINT_NAME}/invocations",
        headers={**w.config.authenticate(), "Content-Type": "application/json"},
        json={"dataframe_split": {"columns": FEATURE_COLUMNS, "data": [features]}},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["predictions"][0]


@app.route("/")
def index():
    return render_template_string(
        PAGE, endpoint=ENDPOINT_NAME, customers=fetch_customers(), result=None
    )


@app.route("/predict", methods=["POST"])
def predict():
    customer_id = request.form.get("customer_id")
    customer = fetch_customer(customer_id)
    risk = predict_churn(customer["features"])
    high = risk > 0.5
    return render_template_string(
        PAGE,
        endpoint=ENDPOINT_NAME,
        customers=fetch_customers(),
        result={
            "customer_id": customer_id,
            "risk_pct": f"{risk * 100:.0f}",
            "css": "high" if high else "low",
            "high": high,
            "verdict": (
                "HIGH risk — this customer is likely to churn."
                if high
                else "Low risk — this customer is unlikely to churn."
            ),
            "action": (
                "Recommended action: contact this customer with a retention offer this week."
                if high
                else "Recommended action: no action needed right now."
            ),
            "recency_days": customer["recency_days"],
            "frequency": customer["frequency"],
            "monetary": customer["monetary"],
        },
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
