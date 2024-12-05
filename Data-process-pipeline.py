# Databricks notebook source
# IMPORTS
from pyspark.sql.functions import col, substring
from pyspark.sql.functions import expr

# COMMAND ----------

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

container = "tp-bronze"
storage_account_name = "name"
storage_account_key = "key"
mount_name = "tp-bronze"

dbutils.fs.mount(
    source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{mount_name}",
    extra_configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": f"{storage_account_key}"
    }
)

# COMMAND ----------

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

container = "tp-silver"
mount_name = "tp-silver"

dbutils.fs.mount(
    source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{mount_name}",
    extra_configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": f"{storage_account_key}"
    }
)

# COMMAND ----------

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

container = "tp-gold"
mount_name = "tp-gold"

dbutils.fs.mount(
    source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{mount_name}",
    extra_configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": f"{storage_account_key}"
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### List csv files

# COMMAND ----------

mount_name = "tp-bronze"
display(dbutils.fs.ls(f"/mnt/{mount_name}/emission/"))
display(dbutils.fs.ls(f"/mnt/{mount_name}/forest_area/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get and display all data

# COMMAND ----------

forest_area_km = f"/mnt/tp-bronze/forest_area/forest_area_km.csv"
forest_area_percent = f"/mnt/tp-bronze/forest_area/forest_area_percent.csv"
Carbon_CO2_Emissions_by_Country = f"/mnt/tp-bronze/emission/Carbon_(CO2)_Emissions_by_Country.csv"
Emission_By_Country = f"/mnt/tp-bronze/emission/Emission By Country.csv"

forest_area_km_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(forest_area_km)
forest_area_percent_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(forest_area_percent)
Carbon_CO2_Emissions_by_Country_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(Carbon_CO2_Emissions_by_Country)
Emission_By_Country_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(Emission_By_Country)

display(forest_area_km_df)
display(forest_area_percent_df)
display(Carbon_CO2_Emissions_by_Country_df)
display(Emission_By_Country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and store initial data to silver delta table

# COMMAND ----------

mount_name = "tp-silver"
forest_area_km_df.withColumnRenamed("Country Name", "country_name").withColumnRenamed("Country Code", "country_code") \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"/mnt/{mount_name}/delta_forest_area_km")

forest_area_percent_df.withColumnRenamed("Country Name", "country_name").withColumnRenamed("Country Code", "country_code") \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"/mnt/{mount_name}/delta_forest_area_percent")

Carbon_CO2_Emissions_by_Country_df.withColumnRenamed("Kilotons of Co2", "kilotons_of_co2").withColumnRenamed("Metric Tons Per Capita", "metric_tons_per_capita") \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"/mnt/{mount_name}/delta_carbon_co2_emissions_by_country")

Emission_By_Country_df.withColumnRenamed("ISO 3166-1 alpha-3", "ISO_3166-1").withColumnRenamed("Per Capita", "per_capita") \
    .write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"/mnt/{mount_name}/delta_emission_by_country")

# COMMAND ----------

mount_name = "tp-silver"
delta_forest_area_km = spark.read.format("delta").load(f"/mnt/{mount_name}/delta_forest_area_km")
delta_forest_area_percent = spark.read.format("delta").load(f"/mnt/{mount_name}/delta_forest_area_percent")
delta_carbon_co2_emissions_by_country = spark.read.format("delta").load(f"/mnt/{mount_name}/delta_carbon_co2_emissions_by_country")
delta_emission_by_country = spark.read.format("delta").load(f"/mnt/{mount_name}/delta_emission_by_country")

# display(delta_forest_area_km)
# display(delta_forest_area_percent)
# display(delta_carbon_co2_emissions_by_country)
# display(delta_emission_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refining the data

# COMMAND ----------

columns_to_check = ["Coal", "Oil", "Gas", "Cement", "Flaring", "Other", "per_capita"]

delta_emission_by_country = delta_emission_by_country.dropna(subset=columns_to_check, how="all") \
    .filter(delta_emission_by_country["Year"] >= 1990) \
    .drop("per_capita")

# ----------- [ FLIPPED YEARS FOR CONSISTENCY ] ----------- #
years = [str(year) for year in range(1990, 2022)]
stack_expr = "stack({0}, ".format(len(years))

for year in years:
    stack_expr += "'{0}', `{0}`, ".format(year)

stack_expr = stack_expr[:-2] + ") as (Year, forest_area_percent)"

delta_forest_area_percent = delta_forest_area_percent.select(
    "country_name",
    "country_code",
    expr(stack_expr)
)

stack_expr = "stack({0}, ".format(len(years))

for year in years:
    stack_expr += "'{0}', `{0}`, ".format(year)
stack_expr = stack_expr[:-2] + ") as (Year, forest_area_km)"

delta_forest_area_km = delta_forest_area_km.select(
    "country_name",
    "country_code",
    expr(stack_expr)
)

# ----------- [ CHANGE YEARS FIELDS FOR CONSISTENCY ] ----------- #

delta_carbon_co2_emissions_by_country = delta_carbon_co2_emissions_by_country.withColumn("Year", substring(col("Date"), 7, 4)).drop("Date")

# ----------- [ RENAMED FIELDS FOR CONSISTENCY ACROSS DATA ] ----------- #

delta_emission_by_country = delta_emission_by_country.withColumnRenamed("ISO_3166-1", "country_code") \
    .withColumnRenamed("Country", "country_name") \
    .withColumnRenamed("Total", "total_CO2_emission") \
    .withColumnRenamed("Coal", "coal_CO2_emission") \
    .withColumnRenamed("Oil", "oil_CO2_emission") \
    .withColumnRenamed("Gas", "gas_CO2_emission") \
    .withColumnRenamed("Cement", "cement_CO2_emission") \
    .withColumnRenamed("Flaring", "flaring_CO2_emission") \
    .withColumnRenamed("per_capita", "per_capita_CO2_emission") \
    .withColumnRenamed("Other", "other_CO2_emission") 

delta_carbon_co2_emissions_by_country = delta_carbon_co2_emissions_by_country.withColumnRenamed("Country","country_name") \
    .withColumnRenamed("Region","region") 

# display(delta_carbon_co2_emissions_by_country)
# display(delta_emission_by_country)
# display(delta_forest_area_percent)
# display(delta_forest_area_km)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Merging the sweet sweet data

# COMMAND ----------

sweet_golden_data = delta_forest_area_km.join(delta_forest_area_percent, on=["country_name", "country_code", "Year"], how="inner") \
    .join(delta_emission_by_country, on=["country_name", "country_code", "Year"], how="inner") \
    .join(delta_carbon_co2_emissions_by_country, on=["country_name", "Year"], how="inner")

# display(sweet_golden_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the data in tp-gold

# COMMAND ----------

mount_name = "tp-gold"
sweet_golden_data.write \
    .mode("overwrite") \
    .format("delta") \
    .save(f"/mnt/{mount_name}/sweet_golden_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount

# COMMAND ----------

mount_name = "tp-bronze"
dbutils.fs.unmount(f"/mnt/{mount_name}")
mount_name = "tp-silver"
dbutils.fs.unmount(f"/mnt/{mount_name}")
mount_name = "tp-gold"
dbutils.fs.unmount(f"/mnt/{mount_name}")
