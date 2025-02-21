from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import split, regexp_replace, col

# Initialize SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("SparkApplicationThirdDelivery") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

# Schema definition
ArticleSchema = StructType([
    StructField("Article_ID", LongType(), True),
    StructField("Title", StringType(), True),
    StructField("DOI", StringType(), True),
    StructField("JournalNumber_ID", LongType(), True),
    StructField("ConferenceBook_ID", LongType(), True),
    StructField("Tags", StringType(), True)
])

# CSV File import to a DataFrame
Article_DF = spark.read.format("CSV") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .schema(ArticleSchema) \
    .load("Article.csv")

# Conversion of the "Tags" field into an array of strings
Article_DF = Article_DF.withColumn("Tags", split(regexp_replace(col("Tags"), r'[

\[\]

"]', ""), ","))

# Print schema
Article_DF.printSchema()

# Show DataFrame
Article_DF.show()
 
#QUERY: Q9: Conference editions that have published two proceedings
# 1. Filtering the edition in a certain period
filter_by_date = CE_DF.filter((CE_DF.Start_date >= "1999-01-01") & (CE_DF.Start_date <= "2010-12-31"))

# 2. Joining the resulting dataframe with the Conference_Book DataFrame
join_with_books = filter_by_date.join(CB_DF, filter_by_date.Conference_ID == CB_DF.ConferenceEdition_ID, "inner")

# 3. Grouping for each edition, considering those that have released exactly two proceedings (having on aggregate operation)
group_for_edition = join_with_books.groupBy(
    join_with_books.Conference_ID,
    join_with_books.Name,
    join_with_books.Start_date,
    join_with_books.End_date
).agg(count(col("Conference_ID")).alias("ConferenceBook_Released")).filter(col("ConferenceBook_Released") == 2)

# 4. Sorting in descending order by number of proceedings released
group_for_edition.sort(col("ConferenceBook_Released").desc()) \
    .select(
        col("Name"), 
        col("Start_date"), 
        col("End_date"), 
        col("ConferenceBook_Released")
    ).show(truncate=False)


# Wait for user input to close the session
text = input("Press Enter to close the session")
if text == "":
    spark.stop()
    print("Session closed")

