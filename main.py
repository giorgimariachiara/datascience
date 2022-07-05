from locale import normalize
# read csv file with pandas

# <<<<<<< Updated upstream
# <<<<<<< Updated upstream
# <<<<<<< Updated upstream
# <<<<<<< Updated upstream
# <<<<<<< Updated upstream
# from pandas import DataFrame, merge 
# =======


# >>>>>>> Stashed changes
# =======


# >>>>>>> Stashed changes
# =======


# >>>>>>> Stashed changes
# =======






from pandas import DataFrame, concat, merge 
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
#from impl2 import RelationalDataProcessor, RelationalQueryProcessor 




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
#print(publication_df.columns)

with open("./relational_db/relational_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)


#----------------------------------------

# Organization DataFrame
crossref = json_doc.get("publishers")
id_and_name = crossref.values()
organization_df = pd.DataFrame(id_and_name)

organization_internal_id = []
for idx, row in organization_df.iterrows():
    organization_internal_id.append("organization-" + str(idx))
organization_df.insert(0, "OrganizationId", Series(organization_internal_id, dtype="string"))

#----------------------------------------

# Venue DataFrame

venue_df = publication_df[["id", "publication_venue", "publisher"]]
venue_df = merge(venue_df, organization_df, left_on="publisher", right_on="id")
venue_df = venue_df[["publication_venue", "OrganizationId"]]
venue_df.drop_duplicates(subset= ["publication_venue", "OrganizationId"], inplace = True)
venue_df = venue_df.reset_index()

venue_internal_id = []
for idx, row in venue_df.iterrows():
    venue_internal_id.append("venue-" + str(idx))
venue_df.insert(0, "VenueId", Series(venue_internal_id, dtype="string"))
venue_df.drop("index", axis=1, inplace = True)

print(venue_df)

#----------------------------------------

# Author dataframe
author = json_doc["authors"]

author_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')

author_df=pd.json_normalize(json.loads(author_df.to_json(orient="records")))
author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

author_df=pd.DataFrame(author_df)

author_df.drop("family_name", axis=1, inplace = True)
author_df.drop("given_name", axis =1, inplace = True)
pd.set_option("display.max_colwidth", None, "display.max_rows", None)


#----------------------------------------


# Person DataFrame

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


#----------------------------------------


# Proceedings DataFrame

proceedings_df = publication_df.query("venue_type =='proceedings'")
proceedings_df = proceedings_df[["id", "publication_venue", "publisher", "event"]]
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
proceedings_df= merge(venue_df, proceedings_df, left_on="publication_venue", right_on="publication_venue")
proceedings_df = proceedings_df[["id", "VenueId", "publisher", "event"]]
proceedings_df = proceedings_df.rename(columns={"VenueId":"publication_venue"})
proceedings_df = proceedings_df.rename(columns={"id":"doi"})

#----------------------------------------


# Cites DataFrame
 
References = json_doc["references"]
cites_df=pd.DataFrame(References.items(),columns=['citing','cited']).explode('cited')
cites_df=pd.json_normalize(json.loads(cites_df.to_json(orient="records")))
cites_df.rename(columns={"References.keys()":"citing","References.values()":"cited"}, inplace = True)

cites_df=pd.DataFrame(cites_df)


#----------------------------------------


# Book Dataframe

book_df = publication_df.query("venue_type =='book'")

book_df= book_df[["id", "publication_venue", "publisher"]]
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
book_df = book_df.rename(columns={"id":"doi"})
book_df= merge(venue_df, book_df, left_on="publication_venue", right_on="publication_venue")
book_df = book_df[["doi", "VenueId", "publisher"]]
book_df = book_df.rename(columns={"VenueId":"publication_venue"})


#----------------------------------------


# Journal DataFrame

journal_df = publication_df.query("venue_type =='journal'")
journal_df= journal_df[["id", "publication_venue", "publisher"]]
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
journal_df = journal_df.rename(columns={"id":"doi"})
journal_df= merge(venue_df, journal_df, left_on="publication_venue", right_on="publication_venue")
journal_df = journal_df[["doi", "VenueId", "publisher"]]
journal_df = journal_df.rename(columns={"VenueId":"publication_venue"})




#----------------------------------------


#  Journal Article DataFrame 2

journal_article_df = publication_df.query("type =='journal-article'")
journal_article_df = journal_article_df[["id", "publication_year", "title", "publication_venue", "issue", "volume"]]
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
#journal_article_df = journal_article_df.rename(columns={"id":"doi"})
journal_article_df= merge(venue_df, journal_article_df, left_on="publication_venue", right_on="publication_venue")
journal_article_df = journal_article_df[["VenueId", "id", "publication_year", "title", "issue", "volume"]]
journal_article_df = journal_article_df.rename(columns={"VenueId":"publication_venue"})

#----------------------------------------

# Book chapter DataFrame


book_chapter_df = publication_df.query("type == 'book-chapter'")
book_chapter_df = book_chapter_df[["id", "publication_year", "title", "publication_venue", "chapter"]]

book_chapter_df = book_chapter_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
book_chapter_df= merge( venue_df, book_chapter_df, left_on="publication_venue", right_on="publication_venue")
book_chapter_df = book_chapter_df[["VenueId", "doi", "title", "publication_year", "chapter"]]
book_chapter_df = book_chapter_df.rename(columns={"VenueId":"publication_venue"})

#----------------------------------------
# Proceedings paper DataFrame

Proceedings_paper_df = publication_df.query("type == 'proceeding-paper'")
Proceedings_paper_df = Proceedings_paper_df[["id", "publication_year", "title", "publication_venue", "issue", "volume"]]
Proceedings_paper_df = Proceedings_paper_df.rename(columns={"id":"doi"})
pd.set_option("display.max_colwidth", None, "display.max_rows", None)
Proceedings_paper_df= merge(venue_df, Proceedings_paper_df, left_on="publication_venue", right_on="publication_venue")

#----------------------------------------
#Venues ext Dataframe
Venue=json_doc["venues_id"]

doi_list = []
issn_isbn_l = []

for key in Venue:
    for item in Venue[key]:
        doi_list.append(key)
        issn_isbn_l.append(item)

venues = pd.DataFrame({
    "doi": Series(doi_list, dtype="string", name="doi"),
    "issn_isbn": Series(issn_isbn_l, dtype="string", name="issn_isbn"),
    
})

venue_ext_dfjournalart = merge(journal_article_df, venues, left_on="id", right_on="doi")
venue_ext_dfjournalart = venue_ext_dfjournalart[["publication_venue", "issn_isbn"]]
venue_ext_dfbookchapter = merge(book_chapter_df, venues, left_on="doi", right_on="doi")
venue_ext_dfbookchapter = venue_ext_dfbookchapter[["publication_venue", "issn_isbn"]]

print(venue_ext_dfbookchapter)


#df_joinVV = merge(venues, venue_df, left_on="doi", right_on = "id") 

#venue_df = df_joinVV[["id", "issn_isbn", "publication_venue", "publisher"]]
#venue_df = venue_df.rename(columns={"issn/isbn":"issn_isbn"})

#Venues_ext_df = 

"""

# Populate the SQL database 
with connect("publication.db") as con:
    venue_df.to_sql("Venueid", con, if_exists="replace", index=False)
    journal_df.to_sql("Journal", con, if_exists="replace", index=False)
    book_df.to_sql("Book", con, if_exists="replace", index=False)
    journal_article_df.to_sql("JournalArticle", con, if_exists="replace", index=False)
    book_chapter_df.to_sql("BookChapter", con, if_exists="replace", index=False)
    proceedings_df.to_sql("Proceedings", con, if_exists="replace", index=False)
    organization_df.to_sql("Organization", con, if_exists="replace", index=False)
    person_df.to_sql("Person", con, if_exists="replace", index=False)
    author_df.to_sql("Authors", con, if_exists="replace", index=False)
    cites_df.to_sql("Cites", con, if_exists="replace", index=False)
    Proceedings_paper_df.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)  
    con.execute("DROP VIEW  IF EXISTS countCited") 
    con.execute("CREATE VIEW countCited AS "
                "SELECT cited, count(*) AS N FROM Cites GROUP BY cited HAVING cited IS NOT NULL;")
    con.execute("DROP VIEW  IF EXISTS maxCited") 
    con.execute("CREATE VIEW maxCited AS "
                "SELECT * FROM countCited WHERE N = (SELECT MAX(N) FROM countCited);")

  

    con.commit()
"""
 
#result_q1 = generic.getPublicationsPublishedInYear(2020)

#print(result_q1)

#print(book_chapter_df)
