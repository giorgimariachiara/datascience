# read csv file with pandas 

import pandas as pd
from pandas import read_csv, Series 

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

for column_name, column in publication_df.items(7):
    print("\nThe name of the current column is", column_name)
    print("The content of the column is as follows:")
    print(column)


# This will create a new data frame starting from 'venues' one,
# and it will include only the column "id"
publication_venueC = publication_df[["publication_venue"]]

# Generate a list of internal identifiers for the venues
publication_venue_internal_id = []
for idx, row in publication_venueC.iterrows():
    publication_venue_internal_id.append("publication_venue-" + str(idx))

# Add the list of venues internal identifiers as a new column
# of the data frame via the class 'Series'
publication_venueC.insert(0, "venueId", Series(publication_venue_internal_id, dtype="string"))

# Show the new data frame on screen
print(publication_venueC)









#create and connect db





<<<<<<< HEAD
=======

>>>>>>> e55092251d9989e3900530fbb6163b0118eb6ed8
