# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(project_root)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python store_excel_data_test_case.py <excel_file_path>")
        sys.exit(1)

    from pymongo import MongoClient
    from fastapi.testclient import TestClient
    from src.com.hpe.chatbot.dl.api import app  # Import FastAPI app


    client = TestClient(app)  # Create a test client -> This will not run the server.

    file_path = sys.argv[1]  # Get CSV file path from CLI

    # Open the file in binary mode and send it to the FastAPI endpoint
    with open(file_path, "rb") as file:
        files = {"file": (file_path, file, "text/csv")}
        client.post("/upload-file/", files=files)


    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["user_input_file_db"] 
    collection = db["user_input_file_parsed_collection"]
    print("File uploaded. Databases:", mongo_client.list_database_names())

