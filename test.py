from locale import normalize
# read csv file with pandas

from implRel import RelationalProcessor
from operator import index
from numpy import index_exp
from pandas import merge 
from collections import deque
import json
from json import load
from sqlite3 import connect
from pprint import pprint
from pandas import read_sql
import pandas as pd
from pandas import read_csv, Series, read_json
from pandas import DataFrame, concat
from extraclasses import Data
import mainRel as pc
import implRel as pc2
import mainGraph as pc3

import os 
dir_path = os.path.dirname(os.path.realpath(__file__))

RelDataPth = dir_path + "/relational_db/"
RelDbPthFN = "Publications.sqlite"
TriStrDataPth = ""
EndpointUrl = ""

rdp = pc.RelationalDataProcessor(RelDbPthFN)
rdp.uploadData(RelDataPth)
tdp = pc3.TriplestoreDataProcessor(EndpointUrl)
tdp.uploadData(TriStrDataPth)
gqp = pc2.GenericQueryProcessor(pc2.QueryProcessor())

print(gqp.getPublicationsByAuthorId("orcid"))
print(gqp.getPublicationsPublishedInYear(2012))
