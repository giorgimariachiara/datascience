# read csv file with pandas 
from sqlite3 import connect
from pprint import pprint
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

#dataframe venue che Luca dice "NON SERVE!!!!!!!!" e luca la vuole chiamare publication veditela tu maria grazie 

# This will create a new data frame starting from 'venues' one,
# and it will include only the column "id"
venue_ids = publication_df[["publication_venue", "id"]]

# Generate a list of internal identifiers for the venues
publication_venue_internal_id = []
for idx, row in venue_ids.iterrows():
    publication_venue_internal_id.append("venueId-" + str(idx))

# Add the list of venues internal identifiers as a new column
# of the data frame via the class 'Series'
venue_ids.insert(0, "venueId", Series(publication_venue_internal_id, dtype="string"))
pd.set_option("display.max_colwidth", None, "display.max_rows", None)



#dataframe of journal = journal_df

journal_df = publication_df.query("venue_type =='journal'")

from pandas import merge 

df_joinJV = merge(journal_df, venue_ids, left_on="id", right_on = "id") 

journal_df = df_joinJV[["venueId", "id", "title", "publisher"]]
journal_df = journal_df.rename(columns={"venueId":"internalId"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)




#dataframe of book = book_df

book_df = publication_df.query("venue_type =='book'")

from pandas import merge 

df_joinBV = merge(book_df, venue_ids, left_on="id", right_on = "id") 

book_df = df_joinBV[["venueId", "id", "title", "publisher"]]
book_df = book_df.rename(columns={"venueId":"internalId"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)



#dataframe di organization = df_organization

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

df_organization=pd.DataFrame(publishers.items(),columns=['id', 'name'])

df_organization=pd.json_normalize(json.loads(df_organization.to_json(orient="records")))
df_organization.rename(columns={"id":"crossref1", "name.id":"crossref","name.name":"name"}, inplace = True)

df_organization= df_organization[["crossref", "name"]]

publishers_internal_id = []
for idx, row in df_organization.iterrows():
    publishers_internal_id.append("publisherId-" + str(idx))

df_organization.insert(0, "publisherId", Series(publishers_internal_id, dtype="string"))
pd.set_option("display.max_colwidth", None, "display.max_rows", None)

print(df_organization)


#dataframe di proceedings = proceedings_df

proceedings_df = publication_df[["id", "title", "publisher", "event"]]


from pandas import merge 

df_joinPO = merge(proceedings_df, df_organization, left_on="publisher", right_on = "crossref") 

proceedings_df = df_joinPO[["id", "title", "event", "publisherId"]]


proceedings_df = proceedings_df.rename(columns={"publisherId":"publisher"})

proceedings_internal_id = []
for idx, row in proceedings_df.iterrows():
    proceedings_internal_id.append("proceedignsId-" + str(idx))

proceedings_df.insert(0, "ProceedingsId", Series(proceedings_internal_id, dtype="string"))
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


print(proceedings_df)

#dataframe journal article = journal_article_df
#manca cite e author e un suo internal id
from pandas import merge 

journal_article_df = publication_df[["id", "publication_year", "title", "publication_venue", "issue", "volume"]]

#join journal article e venue che quindi crea il collegamento anche con journal 
df_joinJAV = merge(journal_article_df, venue_ids, left_on="id", right_on = "id") 

journal_article_df = df_joinJAV[["id", "publication_year", "title", "venueId", "issue", "volume"]]
journal_article_df = journal_article_df.rename(columns={"venueId":"publication_venue"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


#dataframe person con colonna nome, cognome e orcid = df_person
from json import load
import json
import pandas as pd

# importing authors from JSON

from collections import deque

with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


person = json_doc["authors"]

df_person=pd.DataFrame(authors.items(),columns=['doi','author']).explode('author')

df_person=pd.json_normalize(json.loads(df_authors.to_json(orient="records")))
df_person.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

df_person=pd.DataFrame(df_person)

df_person = df_person.drop_duplicates(subset =["orc_id"], keep=False) 
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
df_person.drop("doi", axis =1, inplace = True)


#dataframe author che ha colonna doi e orcid = df_author


from json import load
import json
import pandas as pd

# importing authors from JSON

from collections import deque

with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


person = json_doc["authors"]

df_author=pd.DataFrame(authors.items(),columns=['doi','author']).explode('author')

df_author=pd.json_normalize(json.loads(df_authors.to_json(orient="records")))
df_author.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

df_author=pd.DataFrame(df_author)

df_author.drop("family_name", axis=1, inplace = True)
df_author.drop("given_name", axis =1, inplace = True)
pd.set_option("display.max_colwidth", None, "display.max_rows", None)

print(df_author)

#dataframe book chapter = book_chapter_df
# manca cite e author ma misa che non serve perchè sono tabelle a parte che si uniscono tramite l'id  
from pandas import merge

book_chapter_df = publication_df[["id", "publication_year", "title", "publication_venue", "chapter"]]

df_joinBCV = merge(book_chapter_df, venue_ids, left_on="id", right_on = "id") 

book_chapter_df = df_joinBCV[["id", "publication_year", "title", "venueId", "chapter"]]
book_chapter_df = book_chapter_df.rename(columns={"venueId":"publication_venue"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
#df_joinBCP = merge(book_chapter_df, df_person, left_on="id", right_on = "doi")


#dataframe cites = df_cites
from collections import deque

with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


references = json_doc["references"]

df_cites=pd.DataFrame(references.items(),columns=['cited_doi','citing_doi'])
pd.set_option("display.max_colwidth", None, "display.max_rows", None)

#tentiamo di popolarlo hahaha 
#with connect("publications.db") as con:
    #venue_ids.to_sql("VenueId", con, if_exists="replace", index=False)
    #journal_df.to_sql("Journal", con, if_exists="replace", index=False)
    #book_df.to_sql("Book", con, if_exists="replace", index=False)
    #journal_article_df.to_sql("JournalArticle", con, if_exists="replace", index=False)
    #book_chapter_df.to_sql("BookChapter", con, if_exists="replace", index=False)
    #proceedings_df.to_sql("Proceedings", con, if_exists="replace", index=False)
    #df_organization.to_sql("Organization", con, if_exists="replace", index=False)
    #df_person.to_sql("Person", con, if_exists="replace", index=False)
    #df_author.to_sql("Authors", con, if_exists="replace", index=False)
    #df_cites.to_sql("Cites", con, if_exists="replace", index=False)