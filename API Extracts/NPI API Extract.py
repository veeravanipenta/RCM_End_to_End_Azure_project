# Databricks notebook source
from datetime import date
import requests
from pyspark.sql import SparkSession

# Get the current date
current_date = date.today()

# Initialize Spark session
spark = SparkSession.builder.appName("NPI Data").getOrCreate()

# Base URL for the NPI Registry API
base_url = "https://npiregistry.cms.hhs.gov/api/"

# Parameters for the API request
params = {
    "version": "2.1",  # API version
    "state": "CA",  # Example state
    "city": "Los Angeles",  # Example city
    "limit": 20,  # Limit results for demonstration
}

# Make the initial API request
response = requests.get(base_url, params=params)

if response.status_code == 200:
    npi_data = response.json()
    npi_list = [result["number"] for result in npi_data.get("results", [])]

    # List to store detailed NPI information
    detailed_results = []

    # Loop through each NPI to fetch details
    for npi in npi_list:
        detail_params = {"version": "2.1", "number": npi}
        detail_response = requests.get(base_url, params=detail_params)

        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            if "results" in detail_data and detail_data["results"]:
                for result in detail_data["results"]:
                    npi_number = result.get("number")
                    basic_info = result.get("basic", {})
                    if result["enumeration_type"] == "NPI-1":
                        fname = basic_info.get("first_name", "")
                        lname = basic_info.get("last_name", "")
                    else:
                        fname = basic_info.get("authorized_official_first_name", "")
                        lname = basic_info.get("authorized_official_last_name", "")
                    position = basic_info.get("authorized_official_title_or_position", "")
                    organisation = basic_info.get("organization_name", "")
                    last_updated = basic_info.get("last_updated", "")
                    
                    detailed_results.append({
                        "npi_id": npi_number,
                        "first_name": fname,
                        "last_name": lname,
                        "position": position,
                        "organisation_name": organisation,
                        "last_updated": last_updated,
                        "refreshed_at": current_date,
                    })

    # Create Spark DataFrame and save
    if detailed_results:
        df = spark.createDataFrame(detailed_results)    
        display(df)
        df.write.format("parquet").mode("overwrite").save("abfss://bronze@healthcaresac.dfs.core.windows.net/Npi_data")
        df.write.format("delta").mode("overwrite").saveAsTable("npi_extract")
    else:
        print("No detailed results found.")
else:
    print(f"Failed to fetch data: {response.status_code} - {response.text}")
