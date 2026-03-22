import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

from dashboard.app import create_app

application = create_app()

if __name__ == '__main__':
    application.run(debug=True, port=5000)
