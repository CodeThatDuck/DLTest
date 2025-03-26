# Dynamically add the root project directory to the PYTHONPATH
import sys
import os
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from src.com.hpe.chatbot.dl.exceptions.dao.chatbot_dao_exception import ChatbotDAOException

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../../../'))
sys.path.append(project_root)  

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URL and database name from environment variables
MONGO_URL = os.getenv("MONGO_URL")
USER_INPUT_FILE_DB = os.getenv("USER_INPUT_FILE_DB")

class MongoDBConnection:
    def __init__(self):
        self.client = None
        self.db = None

    async def connect(self):
        """Establishes a MongoDB connection only if not already connected."""
        if self.client is None:
            try:
                self.client = AsyncIOMotorClient(MONGO_URL)
                self.db = self.client[USER_INPUT_FILE_DB]
                print("Connection established: DL to DB")
            except Exception as e:
                raise ChatbotDAOException(f"MongoDB Connection Error: {str(e)}")
        return self.db

    async def close(self):
        """Closes the MongoDB connection if it exists."""
        if self.client:
            try:
                self.client.close()
                self.client = None
            except Exception as e:
                raise ChatbotDAOException(f"Error closing MongoDB connection: {str(e)}")

# Create a single instance to reuse
mongo_connection = MongoDBConnection()

# Access the collection only when needed (lazy loading)
async def get_parsed_collection():
    """Returns the parsed file collection (establishes connection if needed)."""
    return mongo_connection.connect()["user_input_file_parsed_collection"]
