import os
import dotenv 

ROOT_DIR = os.path.dirname(__file__)
dotenv.set_key(".env","ROOT_DIR", ROOT_DIR)
INPUT_DIR = os.path.join(ROOT_DIR, "input")
dotenv.set_key(".env","INPUT_DIR",INPUT_DIR)
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
dotenv.set_key(".env","OUTPUT_DIR",OUTPUT_DIR)

API_KEY = dotenv.get_key(".env","API_KEY")
EXECUTION_MODE = dotenv.get_key(".env","EXECUTION_MODE")
EXTRACTION_MODE = dotenv.get_key(".env","EXTRACTION_MODE")