"""
use_case_tmz model package params
update the environment variables in the '.env'
"""

import os

PROJECT = os.environ.get("PROJECT")
DATASET = os.environ.get("DATASET")
TABLE = os.environ.get("TABLE")
TABLE_TO = os.environ.get("TABLE_TO")
TABLE_10k_SS = os.environ.get("TABLE_10k_SS")
TABLE_10k_AM = os.environ.get("TABLE_10k_AM")
TABLE_DATA_API = os.environ.get("TABLE_DATA_API")

PSI_API_KEY = os.environ.get("PSI_API_KEY")
PSI_API_KEY_1 = os.environ.get("PSI_API_KEY_1")
PSI_API_KEY_2 = os.environ.get("PSI_API_KEY_2")
PSI_API_KEY_3 = os.environ.get("PSI_API_KEY_3")
