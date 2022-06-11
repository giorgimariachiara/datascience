# read csv file with pandas 

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

<<<<<<< Updated upstream
print(publication_df)
=======
#create and connect db

from sqlite3 import connect

with connect("publications.db") as con:
    # do some operation with the new connection
    
    con.commit()  # commit the current transaction to the database


print(csv_file)
>>>>>>> Stashed changes
