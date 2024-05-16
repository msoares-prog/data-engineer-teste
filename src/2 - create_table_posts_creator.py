# Databricks notebook source
file_location = "/FileStore/tables/posts_creator_json-1.gz"

df = spark.read.json(file_location)

table_name = "posts_creator"

df.write.saveAsTable(table_name)