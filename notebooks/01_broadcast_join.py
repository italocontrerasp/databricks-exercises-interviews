# Databricks notebook source
# COMMAND ----------
# Este notebook exportado a .py muestra un ejemplo real de Broadcast Hash Join
# usando dos APIs públicas: REST Countries y DummyJSON.

# COMMAND ----------
# MAGIC %pip install --quiet requests

# COMMAND ----------
import requests
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 1️⃣  Helper functions
# ---------------------------------------------------------------------------

def get_countries_df():
    """
    Descarga la lista completa de países desde REST Countries
    y la convierte en un DataFrame con tres columnas clave.
    """
    url = "https://restcountries.com/v3.1/all"  # :contentReference[oaicite:0]{index=0}
    data = requests.get(url, timeout=30).json()

    rows = [
        {
            "country_code": c.get("cca2"),
            "country_name": c["name"]["common"],
            "region": c.get("region"),
        }
        for c in data
        if c.get("cca2")
    ]

    return spark.createDataFrame(rows)


def get_sales_df(max_pages: int = 10) -> "pyspark.sql.DataFrame":
    """
    Crea un fact table 'sales' combinando los endpoints de DummyJSON:
    - /users  -> para obtener el país textual del usuario
    - /carts  -> para obtener totales de compra
    El parámetro max_pages permite paginar hasta generar > 1 M de filas.
    """
    # --- Users (pais textual) ---------------------------------------------
    users_url = "https://dummyjson.com/users?limit=100&skip={}"  # :contentReference[oaicite:1]{index=1}
    users = []
    for i in range(max_pages):
        users += requests.get(users_url.format(i * 100), timeout=30).json()["users"]

    users_rows = [
        {
            "userId": u["id"],
            "country_code": (u["address"]["country"] or "XX")[:2].upper(),
        }
        for u in users
    ]
    df_users = spark.createDataFrame(users_rows)

    # --- Carts (monto total) ----------------------------------------------
    carts_url = "https://dummyjson.com/carts?limit=100&skip={}"  # :contentReference[oaicite:2]{index=2}
    carts = []
    for i in range(max_pages):
        carts += requests.get(carts_url.format(i * 100), timeout=30).json()["carts"]

    sales_rows = [
        {
            "sale_id": f"{cart['id']}",
            "userId": cart["userId"],
            "amount": float(cart["total"]),
        }
        for cart in carts
    ]
    df_sales = spark.createDataFrame(sales_rows)

    # --- Añade el country_code a partir del usuario ------------------------
    return (
        df_sales.join(df_users, on="userId", how="left")
        .select("sale_id", "country_code", "amount")
        .withColumn("amount", F.round("amount", 2))
    )


# ---------------------------------------------------------------------------
# 2️⃣  Build dimension & fact tables
# ---------------------------------------------------------------------------
df_countries = get_countries_df()
df_sales     = get_sales_df(max_pages=20)   # ~2 000 filas; ajusta para tu demo

# ---------------------------------------------------------------------------
# 3️⃣  Broadcast join
# ---------------------------------------------------------------------------
df_enriched = (
    df_sales.join(broadcast(df_countries), on="country_code", how="left")
    .select("sale_id", "amount", "country_name", "region")
)

# Materializa en Delta (por ejemplo)
(
    df_enriched.write.format("delta")
    .mode("overwrite")
    .save("/tmp/broadcast_join_demo/sales_enriched")
)

# ---------------------------------------------------------------------------
# 4️⃣  Quick check
# ---------------------------------------------------------------------------
print("Plan físico (debería mostrar 'BroadcastHashJoin'):")
df_enriched.explain(mode="simple")
print(f"Total rows after join: {df_enriched.count():,}")
