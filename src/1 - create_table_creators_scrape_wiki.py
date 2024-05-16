# Databricks notebook source
file_location = "/FileStore/tables/wiki_pages_json-2.gz"

df = spark.read.json(file_location)

table_name = "creators_scrape_wiki"

df.write.saveAsTable(table_name)