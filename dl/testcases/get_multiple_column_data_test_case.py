# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(project_root)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python get_multiple_column_data_test_case.py <column_name1> <column_name2> ...")
        sys.exit(1)

    try:
        from pymongo import MongoClient
        from fastapi.testclient import TestClient
        from src.com.hpe.chatbot.dl.api import app  # Import FastAPI app
        from src.com.hpe.chatbot.dl.exceptions.dao.chatbot_dao_exception import ChatbotDAOException

        client = TestClient(app)  # Create a test client (does not run the server)

        column_names = sys.argv[1:]  # Get column names from CLI
        column_names_str = ",".join(column_names)  # Convert to a comma-separated string

        # Call FastAPI endpoint to fetch multiple column data
        response = client.get(f"/data/columns/{column_names_str}")

        if response.status_code == 200:
            print(f"Columns '{column_names}' Data:", response.json())
        else:
            print(f"Error: {response.json()}")

        # Verify the columns exist in MongoDB
        mongo_client = MongoClient("mongodb://localhost:27017/")
        db = mongo_client["user_input_file_db"]
        collection = db["user_input_file_parsed_collection"]

        sample_doc = collection.find_one({}, {**{col: 1 for col in column_names}, "_id": 0})
        missing_columns = [col for col in column_names if col not in sample_doc] if sample_doc else column_names

        if missing_columns:
            print(f"Columns '{missing_columns}' do not exist in the database.")
        else:
            print(f"Columns '{column_names}' exist in the database.")

    except ChatbotDAOException as e:
        print(e)
