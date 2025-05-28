import streamlit as st
import pandas as pd
import mysql.connector



# Set up the Streamlit app
st.header("Agriculture Project")


# Home page


def cleanCSV(df):
        # Remove columns with all NaN or whitespace-only values
       df = df.map(lambda x: x.strip() if isinstance(x, str) else x)  # Strip whitespace from strings
       df = df.dropna(axis=1, how='all')  # Drop columns where all values are NaN
       df = df.loc[:, (df != '').any(axis=0)] # Remove columns with all NaN or whitespace-only values
       return df
         

     
if st.button("Import to SQL"):

       # Database connection
       db_connection = mysql.connector.connect(
       host="localhost",  # Change this to your database host
       user="root",       # Change this to your database username
       password="12345",  # Change this to your database password
       database="agri_crop"  # Change this to your database name
       )

       cursor = db_connection.cursor()

       # Path to the CSV file
       csv_file_path = r"C:\Users\sugan\Downloads\CleanAgri1.csv"
       raw_data = pd.read_csv(csv_file_path)
       # Read the CSV file into a DataFrame
       data = cleanCSV(raw_data)
       st.write(data)
       st.write("Total number of missing values in the dataset:", data.isnull().sum().sum()) #Total number of missing values in the dataset
       # Function to map Pandas dtypes to MySQL data types
       def map_dtype_to_sql(dtype):
              if pd.api.types.is_integer_dtype(dtype):
                     return "INT"
              elif pd.api.types.is_float_dtype(dtype):
                     return "FLOAT"
              elif pd.api.types.is_bool_dtype(dtype):
                     return "BOOLEAN"
              elif pd.api.types.is_datetime64_any_dtype(dtype):
                     return "DATETIME"
              else:
                     return "VARCHAR(255)"  # Default to VARCHAR for strings or unknown types

       # Generate the CREATE TABLE query dynamically based on the CSV columns and datatypes
       table_name = "cropdetails"  # Change this to your desired table name
       columns = ", ".join([f"`{col}` {map_dtype_to_sql(dtype)}" for col, dtype in zip(data.columns, data.dtypes)])
       try:
              create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
              cursor.execute(create_table_query)
       except Exception as e:
              print(f"Error create table query")
              
       # Insert data into the table
       for _, row in data.iterrows():
              try:
                     placeholders = ", ".join(["%s"] * len(row))
                     insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                     cursor.execute(insert_query, tuple(row))
              except Exception as e:
                     print(f"Error inserting row: {row}, Error: {e}")

       # Commit the transaction and close the connection
       db_connection.commit()
       cursor.close()
       db_connection.close()

       print("Data uploaded successfully!")


       
                     