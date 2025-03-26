# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
import math
import pandas as pd
from typing import List, Dict
from src.com.hpe.chatbot.dl.dao.connection.dao_connection import mongo_connection
from src.com.hpe.chatbot.dl.exceptions.dao.chatbot_dao_exception import ChatbotDAOException


# Set up project root path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../..'))
sys.path.append(project_root)

# STORING THE FILE
async def store_file_data(file_path: str):
    """Reads a CSV file and stores its data in MongoDB."""
    if not isinstance(file_path, str):
        raise ChatbotDAOException("file_path must be a string.")

    if not os.path.exists(file_path):
        raise ChatbotDAOException(f"File at {file_path} not found.")

    try:
        df = pd.read_csv(file_path)
        data = df.to_dict("records")
        # Get collection dynamically
        db = await mongo_connection.connect()
        collection = db["user_input_file_parsed_collection"]
        await collection.delete_many({})
        await collection.insert_many(data)

    except Exception as e:
        raise ChatbotDAOException(f"Error processing file: {str(e)}")

# FETCH A PARTICULAR COLUMN
async def get_column_data(column_name: str):
    """Fetches a particular column from MongoDB.."""
    try:
        db = await mongo_connection.connect()
        collection = db["user_input_file_parsed_collection"]

        sample_doc = await collection.find_one({}, {column_name: 1, "_id": 0})
        if not sample_doc or column_name not in sample_doc:
            raise ChatbotDAOException(f"Column '{column_name}' does not exist in the dataset.")

        result = []
        for item in await collection.find({}, {column_name: 1, "_id": 0}):
            value = item.get(column_name)

            # Handle missing values (None, empty string)
            if value is None or value == "":
                raise ChatbotDAOException(f"Column '{column_name}' contains missing values.")

            # Handle NaN and Infinity
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                raise ChatbotDAOException(f"Column '{column_name}' contains NaN or infinite values.")

            result.append(value)

        return result

    except Exception as e:
        raise ChatbotDAOException(f"Issue with the column properties: {str(e)}")
    

async def get_multiple_columns_data(*column_names: str) -> Dict[str, List]:
    """Fetches multiple columns from MongoDB."""
    try:
        # Check column_names to confirm unpacking
        if len(column_names) == 1 and isinstance(column_names[0], list):
            column_names = column_names[0]  # Unwrap the list inside the tuple

        print(f"column_names: {column_names}")  # Debugging line
        db = await mongo_connection.connect()
        collection = db["user_input_file_parsed_collection"]

        result = {column: [] for column in column_names}

        for column in column_names:
            sample_doc = await collection.find_one({}, {column: 1, "_id": 0})
            if not sample_doc or column not in sample_doc:
                raise ChatbotDAOException(f"Column '{column}' does not exist in the dataset.")

        # Merge dictionaries using .update() instead of `|`
        projection = {col: 1 for col in column_names}
        projection["_id"] = 0
        
        for item in await collection.find({}, projection):
            for column in column_names:
                value = item.get(column)

                # Handle missing values
                if value is None or value == "":
                    raise ChatbotDAOException(f"Column '{column}' contains missing values.")

                # Handle NaN and Infinity
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    raise ChatbotDAOException(f"Column '{column}' contains NaN or infinite values.")

                result[column].append(value)

        return result

    except Exception as e:
        raise ChatbotDAOException(f"Issue with the column properties: {str(e)}")


