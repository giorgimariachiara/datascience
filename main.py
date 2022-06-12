# read csv file with pandas 



import pandas as pd
from pandas import read_csv, Series, read_json 

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



# This will create a new data frame starting from 'venues' one,
# and it will include only the column "id"
venue_ids = publication_df[["id"]]

# Generate a list of internal identifiers for the venues
publication_venue_internal_id = []
for idx, row in venue_ids.iterrows():
    publication_venue_internal_id.append("venueId-" + str(idx))

# Add the list of venues internal identifiers as a new column
# of the data frame via the class 'Series'
venue_ids.insert(0, "venueId", Series(publication_venue_internal_id, dtype="string"))

# Show the new data frame on screen



#dataframe of journal 

journal_df = publication_df.query("venue_type =='journal'")

from pandas import merge 

df_joinJV = merge(journal_df, venue_ids, left_on="id", right_on = "id") 

journal_df = df_joinJV[["venueId", "id", "title", "publisher"]]
journal_df = journal_df.rename(columns={"venueId":"internalId"})

#dataframe of book

book_df = publication_df.query("venue_type =='book'")

from pandas import merge 

df_joinBV = merge(book_df, venue_ids, left_on="id", right_on = "id") 

book_df = df_joinBV[["venueId", "id", "title", "publisher"]]
book_df = book_df.rename(columns={"venueId":"internalId"})

#dataframe of proceedings 

proceedings_df = publication_df.query("venue_type =='journal'")

from pandas import merge 

df_joinJV = merge(journal_df, venue_ids, left_on="id", right_on = "id") 

journal_df = df_joinJV[["venueId", "id", "title", "publisher"]]
journal_df = journal_df.rename(columns={"venueId":"internalId"})





#create and connect db

from sqlite3 import connect

with connect("publications.db") as con:
    # do some operation with the new connection
    
    con.commit()  # commit the current transaction to the database




















from json import load
import json
import pandas as pd

# importing authors from JSON

from json import load
import json
import pandas as pd

# importing authors from JSON

from collections import deque

with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


authors = json_doc["authors"]

df_authors=pd.DataFrame(authors.items(),columns=['doi','author']).explode('author')

df_authors=pd.json_normalize(json.loads(df_authors.to_json(orient="records")))
df_authors.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

df_authors=pd.DataFrame(df_authors)



publishers = json_doc["publishers"]

df_publishers=pd.DataFrame(publishers.items(),columns=['id', 'name'])

df_publishers=pd.json_normalize(json.loads(df_publishers.to_json(orient="records")))
df_publishers.rename(columns={"id":"crossref1", "name.id":"crossref","name.name":"name"}, inplace = True)

df_publishersF = df_publishers[["crossref", "name"]]

print(df_publishersF)

















