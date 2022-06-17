from locale import normalize
from operator import index
from colorama import Cursor
from numpy import index_exp
from pandas import DataFrame, merge 
from collections import deque
import json
from json import load
from sqlite3 import connect
from pprint import pprint
import pandas as pd
from pandas import read_csv, Series, read_json



publication_df = pd.read_csv("./relational_db/relational_publication.csv",
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






with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


# organization DataFrame
crossref = json_doc.get("publishers")
id_and_name = crossref.values()
organization_df = pd.DataFrame(id_and_name)

# author dataframe
author = json_doc["authors"]

author_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')

author_df=pd.json_normalize(json.loads(author_df.to_json(orient="records")))
author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

author_df=pd.DataFrame(author_df)

author_df.drop("family_name", axis=1, inplace = True)
author_df.drop("given_name", axis =1, inplace = True)
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


# person DataFrame

person=json_doc["authors"]

doi_l = []
name_orcid_l = []

for key in person:
    for item in person[key]:
        doi_l.append(key)
        name_orcid_l.append(item)

person_l = []

for item in name_orcid_l:
    if item not in person_l:
        person_l.append(item)

family_names_l = []
for item in person_l:
   family_names_l.append(item.get("family"))


given_names_l = []
for item in person_l:
    given_names_l.append(item.get("given"))


orcid_l = []
for item in person_l:
    orcid_l.append(item.get("orcid"))

person_df = pd.DataFrame({
    "orcid": Series(orcid_l, dtype="string", name="orc_id"),
    "given": Series(given_names_l, dtype="string", name="given_name"),
    "family": Series(family_names_l, dtype="string", name="family_name"),
})



# Journal article DataFrame
journal_article_df = publication_df.query("type =='journal-article'")

journal_article_df = journal_article_df[["id", "publication_year", "title", "publication_venue", "issue", "volume"]]
journal_article_df = journal_article_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


# Book chapter DataFrame


book_chapter_df = publication_df.query("type == 'book-chapter'")
book_chapter_df = book_chapter_df[["id", "publication_year", "title", "chapter"]]

book_chapter_df = book_chapter_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


#Proceedings paper DataFrame
Proceedings_paper_df = publication_df.query("type == 'proceeding-paper'")
Proceedings_paper_df = Proceedings_paper_df[["id", "publication_year", "title", "publication_venue", "issue", "volume"]]
Proceedings_paper_df = Proceedings_paper_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)

#dataframe di proceedings
proceedings_df = publication_df.query("venue_type =='proceedings'")
proceedings_df = proceedings_df[["id", "publication_venue", "publisher", "event"]]

pd.set_option("display.max_colwidth", None, "display.max_rows", None)




# Cites DataFrame

# References = json_doc["references"]
# cites_df=pd.DataFrame(References.items(),columns=['cited_doi','citing_doi'])
#pd.set_option("display.max_colwidth", None, "display.max_rows", None)

# Cites DataFrame 2
References = json_doc["references"]
cites_df=pd.DataFrame(References.items(),columns=['citing','cited']).explode('cited')
cites_df=pd.json_normalize(json.loads(cites_df.to_json(orient="records")))
cites_df.rename(columns={"References.keys()":"citing","References.values()":"cited"}, inplace = True)
cites_df=pd.DataFrame(cites_df)
print(cites_df)



# Book Dataframe

book_df = publication_df.query("venue_type =='book'")

book_df= book_df[["id", "publication_venue", "publisher"]]
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
book_df = book_df.rename(columns={"id":"doi"})

# Journal DataFrame

journal_df = publication_df.query("venue_type =='journal'")
journal_df= journal_df[["id", "publication_venue", "publisher"]]
journal_df = journal_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


# Venue DataFrame

Venue=json_doc["venues_id"]

doi_list = []
issn_isbn_l = []

for key in Venue:
    for item in Venue[key]:
        doi_list.append(key)
        issn_isbn_l.append(item)

venues = pd.DataFrame({
    "doi": Series(doi_list, dtype="string", name="doi"),
    "issn/isbn": Series(issn_isbn_l, dtype="string", name="issn/isbn"),
    
})

venue_df = publication_df[["id", "publication_venue", "publisher"]]

df_joinVV = merge(venues, venue_df, left_on="doi", right_on = "id") 

venue_df = df_joinVV[["id", "issn/isbn", "publication_venue", "publisher"]]
#with pd.option_context("display.max_rows", None, "display.max_columns", None):









#tentiamo di popolarlo hahaha 
with connect("publications.db") as con:
    venue_df.to_sql("Venue", con, if_exists="replace", index=False)
    journal_df.to_sql("Journal", con, if_exists="replace", index=False)
    book_df.to_sql("Book", con, if_exists="replace", index=False)
    journal_article_df.to_sql("JournalArticle", con, if_exists="replace", index=False)
    book_chapter_df.to_sql("BookChapter", con, if_exists="replace", index=False)
    proceedings_df.to_sql("Proceedings", con, if_exists="replace", index=False)
    organization_df.to_sql("Organization", con, if_exists="replace", index=False)
    person_df.to_sql("Person", con, if_exists="replace", index=False)
    author_df.to_sql("Authors", con, if_exists="replace", index=False)
    #cites_df.to_sql("Cites", con, if_exists="replace", index=False)
    Proceedings_paper_df.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)   

    con.commit()



#print(cites_df)

