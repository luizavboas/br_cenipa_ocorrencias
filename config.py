import os
import dotenv 
dotenv.load_dotenv()

ROOT_DIR = os.path.dirname(__file__)
os.environ["ROOT_DIR"] = ROOT_DIR
INPUT_DIR = os.path.join(ROOT_DIR, "input")
os.environ["INPUT_DIR"] = INPUT_DIR
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")
os.environ["OUTPUT_DIR"] = OUTPUT_DIR