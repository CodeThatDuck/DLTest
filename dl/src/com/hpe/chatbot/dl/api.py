# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../'))
sys.path.append(project_root)

import shutil  # For temporary storing of the file
from fastapi import FastAPI, UploadFile, File
from src.com.hpe.chatbot.dl.dao import chatbot_dao  # Invoke the chatbot_dao which further invokes the dao_connection
from src.com.hpe.chatbot.dl.exceptions.dao.chatbot_dao_exception import ChatbotDAOException
from src.com.hpe.chatbot.dl.dao.connection.dao_connection import mongo_connection

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()


# Add CORS middleware to allow requests from the frontend React app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For now allows any domain to send requests. But the following is the frontend port number. Allow React app to make requests.
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)


@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    """Uploads an Excel file and stores it in MongoDB."""
    try:
        file_location = f"./temp/{file.filename}"
        
        # Save file temporarily
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Process and store in MongoDB
        await chatbot_dao.store_file_data(file_location)

    except ChatbotDAOException as e:
        raise e

    except Exception as e:
        raise ChatbotDAOException(f"Error uploading file: {str(e)}")
    
@app.get("/data/column/{column_name}")
async def get_column(column_name: str):
    """Fetches a particular column from the database."""
    try:
        return {"column": column_name, "values": await chatbot_dao.get_column_data(column_name)}
    
    except ChatbotDAOException as e:
        raise e
    
    except Exception as e:
        raise ChatbotDAOException(f"Issue with the column properties: {str(e)}")


@app.get("/data/columns/{column_names}")
async def get_multiple_columns(column_names: str):
    """Fetches multiple columns from the database."""
    try:
        column_list = column_names.split(",")  # Convert string to list
        return {"columns": column_list, "values": await chatbot_dao.get_multiple_columns_data(column_list)}
    
    except ChatbotDAOException as e:
        raise e
    
    except Exception as e:
        raise ChatbotDAOException(f"Issue with the column properties: {str(e)}")
