import os
from .shared import *

NFS_STORAGE_ADDRESS="10.26.0.16"
NFS_RETRIEVAL_ADDRESS="10.25.0.16"

#This tries to reach an API running locally.
MOUSERA_API_URL="http://127.0.0.1:8000/api/v1/"
MOUSERA_API_URL_EXTERNAL="http://127.0.0.1:8000/api/v1/"

#To hit the staging API instead, uncomment these. Both are pointed at 'external' since only machines on the right
#subnet inside AWS can access the internal ELB
# MOUSERA_API_URL="http://api-external-nonprod.mousera.com/api/v1/"
# MOUSERA_API_URL_EXTERNAL="http://api-external-nonprod.mousera.com/api/v1/"

#Can't reach internal from outside...
KAIROS2_INTERNAL_HOST = "kairosdb2-external-nonprod.mousera.com"

#Add /usr/local/bin to our path
os.environ.update({"PATH": os.environ['PATH']+":/usr/local/bin"})
