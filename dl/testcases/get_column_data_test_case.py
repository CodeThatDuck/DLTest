# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(project_root)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get_column_data_test_case.py <column_name>")
        sys.exit(1)

    try:
        from pymongo import MongoClient
        from fastapi.testclient import TestClient
        from src.com.hpe.chatbot.dl.api import app  # Import FastAPI app
        from src.com.hpe.chatbot.dl.exceptions.dao.chatbot_dao_exception import ChatbotDAOException
    
        client = TestClient(app)  # Create a test client (does not run the server)

        column_name = sys.argv[1]  # Get column name from CLI

        # Call FastAPI endpoint to fetch column data
        response = client.get(f"/data/column/{column_name}")

        if response.status_code == 200:
            print(f"Column '{column_name}' Data:", response.json())
        else:
            print(f"Error: {response.json()}")

        # Verify the column exists in MongoDB
        mongo_client = MongoClient("mongodb://localhost:27017/")
        db = mongo_client["user_input_file_db"]
        collection = db["user_input_file_parsed_collection"]

        sample_doc = collection.find_one({}, {column_name: 1, "_id": 0})
        if sample_doc and column_name in sample_doc:
            print(f"Column '{column_name}' exists in the database.")
        else:
            print(f"Column '{column_name}' does not exist in the database.")

    except ChatbotDAOException as e:
        print(e)