from pyspark.sql import SparkSession
import os
import google.generativeai as genai
from dotenv import load_dotenv


parquet_data_path = r"Parquest_filepath"


load_dotenv()


GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not os.path.exists(parquet_data_path):
    print(f"Error: Parquet data path not found: {parquet_data_path}")
    print("Please ensure the synthetic_loan_data_parquet directory exists and contains your Parquet files.")
    exit()

if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY environment variable not set.")
    print("Please set the GEMINI_API_KEY environment variable or add it to a .env file.")
    exit()

genai.configure(api_key=GEMINI_API_KEY)


model = genai.GenerativeModel('gemini-2.0-flash')


spark = SparkSession.builder \
    .appName("SparkSQLBot") \
    .getOrCreate()


df = spark.read.parquet(parquet_data_path)


df.createOrReplaceTempView("loan_data")

print("Spark DataFrame loaded and registered as 'loan_data' view.")
print("Schema of the loaded DataFrame:")
df.printSchema()
print("\nFirst 5 rows of the loaded DataFrame:")
df.show(5)

def get_sql_query_from_gemini(question, table_schema, table_name="loan_data"):

    prompt = f"""
    You are a helpful assistant that converts natural language questions into Spark SQL queries.
    The table name is `{table_name}`.
    The table schema is as follows:
    {table_schema}
    
    there is a sanction date column (YYYY-MM-DD). the dates here are generally divided into quarters as follows :
    
    for every year, 
    
    Q1 20XX = January, February, March of 20XX. This can also be referred to as JFM 20XX
    Q2 20XX = April, May, June of 20XX. This can also be referred to as AMJ 20XX
    Q3 20XX = July, August, September of 20XX. This can also be referred to as JAS 20XX
    Q4 20XX = October, November, December of 20XX. This can also be referred to as OND 20XX.
    
    Use the sanction date column to perform time based analysis/comparisions.
    
    
    Please generate a Spark SQL query for the following question.
    Only return the SQL query, without any additional text or explanations.
    If the question cannot be answered with the given schema, return "N/A".
    
   
    Question: {question}
    Spark SQL Query:
    """
    response = model.generate_content(prompt)
    sql_query = response.text.strip()


    if sql_query.startswith("```sql") and sql_query.endswith("```"):
        sql_query = sql_query[6:-3].strip()
    elif sql_query.startswith("```") and sql_query.endswith("```"):
        sql_query = sql_query[3:-3].strip()

    return sql_query

def get_table_schema_string(spark_dataframe):

    schema_str = ""
    for field in spark_dataframe.schema:
        schema_str += f"- {field.name}: {field.dataType}\n"
    return schema_str


table_schema_str = get_table_schema_string(df)


print("\n--- Spark SQL Insight Bot ---")
print("Type 'exit' to quit.")

while True:
    user_question = input("\nYour question: ")
    if user_question.lower() == 'exit':
        print("Exiting bot. Goodbye!")
        break


    sql_query_generated = get_sql_query_from_gemini(user_question, table_schema_str)


    if sql_query_generated.lower() == "n/a":
        print("Sorry, I cannot generate an answer for that question with the available data.")
    else:

        result_df = spark.sql(sql_query_generated)
        print("\nHere's your insight:")
        result_df.show(truncate=False)


spark.stop()
