import pandas as pd
from json import load, loads
from sqlite3 import connect

class Data:
    def __init__(self, pth, csv, jsn):
        # read CSV
        Publications0DF = pd.read_csv(pth + csv,
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
        with open(pth + jsn, "r", encoding="utf-8") as f:
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
        # print(AuthorsDF.head(7))

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

        """
        'Venue' table has to be created 
            selecting the columns in 'Publications0DF'
            dropping duplicates
            adding an 'id'
        'Venues_idDF' 
            has more than one 'id' for each venue
            is connected to the venue through the DOI in 'Publications0DF'
        Solution 1 - create a local 'venue-id' (cons: same venue can have different 'id' in different 'Data' instances)
        Solution 2 - use min(external_venue_id)
        """

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

