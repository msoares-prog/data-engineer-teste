# Databricks notebook source
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import json

# COMMAND ----------

def extract_youtube_user_name(text):
    try:
        match = re.search(r'https://www.youtube.com/user/([\w-]+)', text)
        if match:
            return match.group(1)
        else:
            return None
    except Exception as e:
        print(f"Error extracting YouTube user name: {e}")
        return None

# COMMAND ----------

def get_youtube_user_id(wiki_name):
    try:
        url = "https://en.wikipedia.org/w/api.php"
        params = {"action": "parse", "page": wiki_name, "format": "json"}
        response = requests.get(url, params=params)
        response.raise_for_status() 
        data = response.json()
        
        if 'parse' in data and 'text' in data['parse']:
            text = json.dumps(data['parse']['text'])
            user_name = extract_youtube_user_name(text)
            return user_name
        return None
    except requests.exceptions.RequestException as e:
        print(f"HTTP request error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    except Exception as e:
        print(f"Error retrieving YouTube user ID: {e}")
        return None

# COMMAND ----------

table_name = 'creators_scrape_wiki'

df_wiki_page = spark.read.table(table_name)

youtube_user_id_udf = udf(get_youtube_user_id, StringType())

df_users_yt = df_wiki_page.withColumn("user_id", youtube_user_id_udf(df_wiki_page["wiki_page"]))

df_users_yt.write.saveAsTable("users_yt")
