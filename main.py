# from impl import nomi classi



# read csv file with pandas 

from os import listxattr
import pandas as pd
from pandas import read_csv, Series, DataFrame

publication_df = pd.read_csv("./relational_db/relational_publication.csv", keep_default_na=False,
                        dtype={
                                    "id": "string",
                                    "title": "string",
                                    "type": "string",
                                    "publication_year": "string",
                                    "issue": "string",
                                    "volume": "string",
                                    "chapter": "string",
                                    "publication_venue": "string",
                                    "venue_type": "string",
                                    "publisher": "string",
                                    "event": "string"

                        },encoding="utf-8")







