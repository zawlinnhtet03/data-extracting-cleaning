import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables (if you are using .env for storing credentials)
load_dotenv()

# Database connection details (use environment variables or hardcode if necessary)
db_connection = mysql.connector.connect(
    host=os.getenv("DB_HOST"),     
    user=os.getenv("DB_USER"),      
    password=os.getenv("DB_PASSWORD"),  
    database=os.getenv("DB_NAME"),  
    port=os.getenv("DB_PORT")  
)

# SQL query to join participants and participant_sessions
query = """
    SELECT        
        participants.last_name,
        participants.first_name,
        participants.is_nuclear_medicine_member,
        participants.occupation_category,
        participants.license_number,
        participants.is_medical_specialist_member,
        participants.organization,
        participants.work_registration_number,
        participants.email,
        participants.phone_number,
        participant_sessions.check_in_time,
        participant_sessions.check_out_time
    FROM 
        participants
    LEFT JOIN 
        participant_sessions 
    ON 
        participants.id = participant_sessions.participant_id;
"""

# Step 1: Execute the query and fetch the results
cursor = db_connection.cursor(dictionary=True)  # Use dictionary for column names
cursor.execute(query)
rows = cursor.fetchall()

# Step 2: Convert the result to a pandas DataFrame
df = pd.DataFrame(rows)

# Step 3: Save the DataFrame to CSV or Excel
df.to_csv("participants_sessions_data.csv", index=False, encoding="utf-8")
# df.to_excel("participants_sessions_data.xlsx", index=False)

# Step 4: Close the cursor and database connection
cursor.close()
db_connection.close()

print("Data exported successfully!")
