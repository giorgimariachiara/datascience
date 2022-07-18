import json
from numpy import cross
import pandas as pd
from json import load, loads
from sqlite3 import connect
from pandas import DataFrame, Series, merge
import os.path
from mainRel import Venue


csv = "./graph_db/graph_publications.csv"
jsn = "./graph_db/graph_other_data.json"
#pth, 

"""
class Data:
    def __init__(self, csv, jsn):
        # read CSV
        Publications0DF = pd.read_csv(csv, keep_default_na= False, #potrebbe dare problemi
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
                        },encoding="utf-8")\
            .rename(columns={"publication_year" : "publicationYear"})

        # read JSON
        with open(jsn, "r", encoding="utf-8") as f:
            json_doc = load(f)
        
        # extract and flatten individual dictionaries

        # --- Questo pivotaggio/verticalizzazione non si può vedere, 
        # ----ma non sono riuscito a migliorare
        AuthorsDF = pd.DataFrame(json_doc["authors"].items(), 
                            columns=['doi','author']).explode('author')
        AuthorsDF = pd.json_normalize(loads(AuthorsDF.to_json(orient="records")))
        AuthorsDF.rename(columns={
            "author.family":"familyName",
            "author.given":"givenName",
            "author.orcid":"orcid"}, 
            inplace = True)
        

        ReferencesDF = json_doc["references"]
        ReferencesDF = pd.DataFrame(ReferencesDF.items(),columns=['citing','cited'])\
                        .explode('cited')
        ReferencesDF=pd.json_normalize(loads(ReferencesDF.to_json(orient="records")))
        ReferencesDF.rename(columns={"References.keys()":"citing",
                                    "References.values()":"cited"}, inplace = True)

        PublishersDF = pd.DataFrame.from_dict(json_doc["publishers"], orient='index')

        Venues_idDF = pd.DataFrame.from_dict(json_doc["venues_id"], orient='index', columns = ['ext_id1', 'ext_id2'])
#+++++++++++++
# function to archive data in the RDB
# def Rdata2SQLite(Rdata, RDBpth):

        self.Organization = PublishersDF
        self.cites = ReferencesDF.dropna()

        Authors1DF = AuthorsDF.assign(id = lambda x: 'orcid:' + x.orcid)\
                .drop(columns="orcid")\
                .rename(columns={"id" : "orcid"})
        
        self.Person = Authors1DF.filter(items=["orcid", "family_name", "given_name"])\
                .drop_duplicates()\
                .rename(columns={"orcid" : "id", "given_name" : "givenName", "family_name" : "familyName"})\
                .reindex(["id", "givenName", "familyName"], axis = "columns")
        #        .insert(0, 'id', 'orcid:' + x.["orcid"])\

        self.author = Authors1DF.filter(items=["doi", "orcid"]).drop_duplicates() # that should not exist!

        
        'Venue' table has to be created 
            selecting the columns publication_venue, publisher, in 'Publications0DF'
            dropping duplicates
            adding an 'id'
        'Venues_idDF' 
            has more than one 'id' for each venue
            is connected to the venue through the DOI in 'Publications0DF'
        Solution 1 - create a local 'venue-id' (cons: same venue can have different 'id' in different 'Data' instances)
        Solution 2 - use min(external_venue_id)
        
        
        
        # -- WARNING if there are 'publications' without correspondance in 'venues_id'
        no_corrs = pd.Index(Publications0DF['id']).difference(pd.Index(Venues_idDF.index))
        if len(no_corrs) > 0:
            print(" *** WARNING *** Following publications have no 'venue_id'")
            print(Publications0DF[Publications0DF["id"].isin(no_corrs)])
        else:
            print(" --- OK: All pubblications have an entry in 'venues_id'")

        # -- WARNING if 'publication_venue's don't have unique 'venue_type's and 'publisher' 
        not_unq = Publications0DF.filter(items=['publication_venue', 'venue_type', 'publisher'])\
            .drop_duplicates()\
            .groupby("publication_venue")\
            .count()\
            .query('venue_type >1 or publisher >1')
        if len(not_unq) > 0:
            print(" *** WARNING - 'venue_type' or 'publisher' are not unique for each 'publication_venue'")
            print(not_unq)
        else:
            print(" --- OK: 'publication_venue's have unique 'venue_type's and 'publisher'")

        # *** Solution 1 - ABANDONED
        # VenuesDF = Venues_tDF
        # VenuesDF.insert(0, 'id', range(0, VenuesDF.shape[0]))
        # VenuesDF['id'] = VenuesDF['id'].apply(lambda x: 'venue-' + str(int(x)))
        # print(VenuesDF.describe(include='all') )
        # print(VenuesDF.head(5) )

        # *** Solution 2
        # Merge venue info from publications with 'venues_id'
        veid = pd.merge(Publications0DF.filter(items=["id", "publication_venue", 'venue_type', 'publisher']),\
                        Venues_idDF, left_on = "id", right_index = True)\
                    .drop(columns="id")\
                    .drop_duplicates()
        # Pivot 'external_ids' to 'id', select MIN() to make it unique
        self.Venue = pd.concat([
            veid.drop(columns="ext_id2").rename(columns={"ext_id1" : "id"}),
            veid.drop(columns="ext_id1").rename(columns={"ext_id2" : "id"}),
            ]).dropna()\
                .groupby(["publication_venue"], as_index=False).min()\
                .rename(columns={"publication_venue" : "title", "publisher" : "publisherId"})\
                .reindex(["id", "title", "venue_type", "publisherId"], axis = "columns")
        #print(PublishersDF)
        # Substitute 'publication_venue' with 'venuedId'
        Publications1DF = pd.merge(
            Publications0DF.filter(items=
                ["id", "publicationYear", "title", "type", "event", "publication_venue"]),
            self.Venue.rename(columns={"id" : "publicationVenueId", 
                                    "title" : "publication_venue"}), 
            on="publication_venue")\
            .drop(columns=["publication_venue", "publisherId"])
        self.Publication = Publications1DF.drop(columns=["event", "venue_type"])

        self.JournalArt0 = Publications0DF.query('type == "journal-article"')\
            .filter(items=["id", "issue", "volume"])
        self.BookChapter0 = Publications0DF.query('type == "book-chapter"')\
            .filter(items=["id", "chapter"])
        # 'ProceedingsPapers' table will be created as VIEW, filtering 'Publications'

        # 'Journals' table will be created as VIEW, filtering 'Venues'
        # 'Books' table will be created as VIEW, filtering 'Venues'
        self.Proceeding0 = Publications1DF\
            .query('venue_type == "proceedings"')\
            .filter(items=["id", "event"])

#+++++++++++++ end of Data class
#oggetto = Data(csv, jsn)
#print(oggetto.Publication) 

nomi = []
for column in oggetto.PublishersDF:
    nomi.append(column)
print(nomi)
"""
class Data:
    def __init__(self, csv, jsn):

        if os.path.exists(csv):
            PublicationsDF = pd.read_csv(csv, keep_default_na= False,
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
                        },encoding="utf-8")\
                .rename(columns={"publication_year" : "publicationYear"})

        else:
            print("WARNING: CSV file '" + csv + "' does not exist!")
        
        if os.path.exists(jsn):
        # read JSON 
            with open(jsn, "r", encoding="utf-8") as f:
                json_doc = load(f)
        else:
            print("WARNING: JSON file '" + jsn + "' does not exist!")


        #AUTHOR DATAFRAME
        author = json_doc["authors"]
        author_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')
        author_df=pd.json_normalize(json.loads(author_df.to_json(orient="records")))
        author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
        author_df.drop("family_name", axis=1, inplace = True)
        author_df.drop("given_name", axis =1, inplace = True)
        self.Author_DF = author_df

        #CITES DATAFRAME
        References = json_doc["references"]
        cites_df=pd.DataFrame(References.items(),columns=['citing','cited']).explode('cited')
        cites_df=pd.json_normalize(json.loads(cites_df.to_json(orient="records")))
        cites_df.rename(columns={"References.keys()":"citing","References.values()":"cited"}, inplace = True)
        self.Cites_DF = cites_df   #qui è da vedere se siamo sicuri di voelr togliere quelli che non citano nulla (o dobbiamo scrivere che se non lo trovi non cito nulla)

        #PERSON DATAFRAME 
        author = json_doc["authors"]
        person_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')
        person_df=pd.json_normalize(json.loads(person_df.to_json(orient="records")))
        person_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
        self.Person_DF = person_df

        #VENUE DATAFRAME
        venues_df = pd.DataFrame.from_dict(json_doc["venues_id"], orient='index', columns = ['id1', 'id2'])
        veid = pd.merge(PublicationsDF.filter(items=["id", "publication_venue", 'venue_type', 'publisher']),\
                        venues_df, left_on = "id", right_index = True)\
                    .drop(columns="id")\
                    .drop_duplicates()

        # Pivot 'external_ids' to 'id', select MIN() to make it unique
        
        venue_df = pd.concat([
            veid.drop(columns="id2").rename(columns={"id1" : "venueid"}),
            veid.drop(columns="id1").rename(columns={"id2" : "venueid"}),
            ]).dropna()\
                .groupby(["publication_venue"], as_index=False).min()\
                .rename(columns={"publication_venue" : "title", "publisher" : "publisherId"})\
                .reindex(["venueid", "title", "venue_type", "publisherId"], axis = "columns")
        self.Venue_DF = venue_df
        #da chiedere il funzionamento

        #ORGANIZATION DATAFRAME 
        crossref = json_doc["publishers"]
        id_and_name = crossref.values()
        organization_df = pd.DataFrame(id_and_name)
        self.Organization_DF = organization_df

        #PUBLICATION DATAFRAME 
        
        publication_df = PublicationsDF[["id", "title", "type", "publicationYear","publisher", "publication_venue"]]
        publication_df = pd.merge(venue_df, publication_df, left_on="title", right_on="publication_venue")
        publication_df = publication_df[["id", "title_x", "type", "publicationYear", "publisherId"]]\
                        .rename(columns={"publisherId" : "PublicationVenueId", "title_x":"title"})
        self.Publication_DF = publication_df

        #BOOK CHAPTER DATAFRAME
        book_chapter_df = PublicationsDF.query("type == 'book-chapter'")
        book_chapter_df = book_chapter_df[["id", "chapter"]]
        self.Book_chapter_DF = book_chapter_df

        #JOURNAL ARTICLE DATAFRAME
        journal_article_df = PublicationsDF.query("type == 'journal-article'")
        journal_article_df = journal_article_df[["id", "issue", "volume"]]
        self.Journal_article_DF = journal_article_df

        #PROCEEDINGS PAPER DATAFRAME 
        proceedings_paper_df = PublicationsDF.query("type == 'proceeding-paper'")
        proceedings_paper_df = proceedings_paper_df[["id"]]
        self.Proceedings_paper_DF = proceedings_paper_df

        #BOOK DATAFRAME
        self.Book_DF = venue_df.query("venue_type == 'book'")

        #JOURNAL DATAFRAME
        self.Journal_DF= venue_df.query("venue_type == 'journal'")
        
        #PROCEEDINGS DATAFRAME
        proceedings_df = PublicationsDF[["id", "publication_venue", "event"]]
        proceedings_df = pd.merge(venue_df, proceedings_df, left_on="title", right_on="publication_venue")\
            .query("venue_type == 'proceedings'")
        self.Proceedings_DF = proceedings_df[["venueid", "title", "event", "publisherId"]]
        
        #VENUE EXTERNAL ID
        Venue=json_doc["venues_id"]

        doi_list = []
        issn_isbn_l = []

        for key in Venue:
            for item in Venue[key]:
                doi_list.append(key)
                issn_isbn_l.append(item)

        self.VenuesExt_DF = pd.DataFrame({
            "doi": Series(doi_list, dtype="string", name="doi"),
            "issn_isbn": Series(issn_isbn_l, dtype="string", name="issn_isbn"),
            
        })

oggetto = Data(csv, jsn)
ogge = oggetto.VenuesExt_DF
ogge2 = oggetto.Person_DF


print(ogge)

