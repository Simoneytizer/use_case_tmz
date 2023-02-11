"""
use_case_tmz model package params
update the environment variables in the '.env'
"""

import os

PROJECT = os.environ.get("PROJECT")
DATASET = os.environ.get("DATASET")
TABLE = os.environ.get("TABLE")
TABLE_TO = os.environ.get("TABLE_TO")

PSI_API_KEY = os.environ.get("PSI_API_KEY")
