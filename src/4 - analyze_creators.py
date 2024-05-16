# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, from_unixtime, desc, rank, explode, count, sum, when
from pyspark.sql.window import Window
from itertools import product

# COMMAND ----------

def read_data(spark, users_yt_table, posts_creator_table):
    try:
        df_users_yt = spark.read.table(users_yt_table)
        df_posts_creator = spark.read.table(posts_creator_table)
        df_posts_creator = df_posts_creator.withColumnRenamed('yt_user', 'user_id')
        df_posts_creator = df_posts_creator.withColumn("published_at_date", from_unixtime(col("published_at")).cast("date"))
        df_joined = df_posts_creator.join(df_users_yt, "user_id")
        return df_users_yt, df_posts_creator, df_joined
    except Exception as e:
        print(f"Error reading data: {e}")
        raise

# COMMAND ----------

# top 3 posts ordenado por likes de cada creator nos últimos 6 meses (user_id, title, likes, rank)
def get_top_posts_by_likes(df_joined):
    try:
        window_likes = Window.partitionBy("user_id").orderBy(desc("likes"))
        df_top_likes = df_joined.withColumn("rank_likes", rank().over(window_likes)).filter(col("rank_likes") <= 3)
        return df_top_likes.select(['user_id', 'title', 'likes', 'rank_likes'])
    except Exception as e:
        print(f"Error getting top posts by likes: {e}")
        raise

# COMMAND ----------

# top 3 posts ordenado por views de cada creator nos últimos 6 meses (user_id, title, views, rank)
def get_top_posts_by_views(df_joined):
    try:
        window_views = Window.partitionBy("user_id").orderBy(desc("views"))
        df_top_views = df_joined.withColumn("rank_views", rank().over(window_views)).filter(col("rank_views") <= 3)
        return df_top_views.select(['user_id', 'title', 'views', 'rank_views'])
    except Exception as e:
        print(f"Error getting top posts by views: {e}")
        raise

# COMMAND ----------

# Mostrar os yt_user que estão na tabela default.post_creator mas não estão na tabela default.users_yt
def get_missing_users(df_posts_creator, df_users_yt):
    try:
        return df_posts_creator.join(df_users_yt, "user_id", "left_anti").select("user_id")
    except Exception as e:
        print(f"Error getting missing users: {e}")
        raise

# COMMAND ----------

# Mostrar a quantidade de publicações por mês de cada creator
def get_monthly_post_counts(df_posts_creator):
    try:
        df_posts_creator = df_posts_creator.withColumn("year", year("published_at_date")).withColumn("month", month("published_at_date"))
        return df_posts_creator.groupBy("user_id", "year", "month").agg(count("*").alias("posts_count"))
    except Exception as e:
        print(f"Error getting monthly post counts: {e}")
        raise

# COMMAND ----------

# Exercício Extra 1: mostrar 0 nos meses que não tem video
def get_monthly_post_counts_with_zeros(df_posts_creator):
    try:
        df_posts_creator = df_posts_creator.withColumn("year", year("published_at_date")).withColumn("month", month("published_at_date"))
        years = df_posts_creator.select("year").distinct().rdd.flatMap(lambda x: x).collect()
        months = list(range(1, 13))
        user_ids = df_posts_creator.select("user_id").distinct().rdd.flatMap(lambda x: x).collect()
        combinations = list(product(user_ids, years, months))
        df_combinations = spark.createDataFrame(combinations, ["user_id", "year", "month"])
        df_monthly_posts_count = df_posts_creator.groupBy("user_id", "year", "month").count()
        df_result = df_combinations.join(df_monthly_posts_count, ["user_id", "year", "month"], "left_outer").na.fill(0)
        return df_result.orderBy("user_id", "year", "month")
    except Exception as e:
        print(f"Error getting monthly post counts with zeros: {e}")
        raise

# COMMAND ----------

# Exercício Extra 2: transformar a tabela no formato que a primeira coluna é o user_id e temos uma coluna para cada mês.
def pivot_monthly_post_counts(df_monthly_posts_count):
    try:
        months = [str(i).zfill(2) for i in range(1, 13)]
        return df_monthly_posts_count.groupBy("user_id").pivot("year").agg(*[sum(when(col("month") == m, col("posts_count")).otherwise(0)).alias(m) for m in months])
    except Exception as e:
        print(f"Error pivoting monthly post counts: {e}")
        raise

# COMMAND ----------

#Exercício Extra 3: Mostrar as 3 tags mais utilizadas por criador de conteúdo
def get_top_3_tags(df_posts_creator):
    try:
        df_exploded_tags = df_posts_creator.select("creator_id", explode("tags").alias("tag"))
        df_tag_counts = df_exploded_tags.groupBy("creator_id", "tag").count()
        window = Window.partitionBy("creator_id").orderBy(desc("count"))
        df_ranked_tags = df_tag_counts.withColumn("rank", rank().over(window))
        return df_ranked_tags.filter(col("rank") <= 3).orderBy("creator_id", "rank")
    except Exception as e:
        print(f"Error getting top 3 tags: {e}")
        raise

# COMMAND ----------

# Main
users_yt_table = 'users_yt'
posts_creator_table = 'posts_creator'

df_users_yt, df_posts_creator, df_joined = read_data(spark, users_yt_table, posts_creator_table)

df_top_likes = get_top_posts_by_likes(df_joined)
df_top_views = get_top_posts_by_views(df_joined)
df_missing_users = get_missing_users(df_posts_creator, df_users_yt)
df_monthly_posts_count = get_monthly_post_counts(df_posts_creator)
df_monthly_posts_count_with_zeros = get_monthly_post_counts_with_zeros(df_posts_creator)
df_pivoted = pivot_monthly_post_counts(df_monthly_posts_count)
df_top_3_tags = get_top_3_tags(df_posts_creator)

display(df_top_likes)
display(df_top_views)
display(df_missing_users)
display(df_monthly_posts_count)
display(df_monthly_posts_count_with_zeros)
display(df_pivoted)
display(df_top_3_tags)