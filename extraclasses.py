from importlib.resources import path
import json
from numpy import cross
import pandas as pd
from json import load, loads
from sqlite3 import connect
from pandas import DataFrame, Series, merge
import os.path

class DataCSV(object): 
    def __init__(self, csv):
        #self.csv = csv

        if os.path.exists(csv):
            PublicationsDF = pd.read_csv(csv , keep_default_na= False,
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

            # DATAFRAME FROM CSV

            #PUBLICATION DATAFRAME 
            
            self.Publication_DF = PublicationsDF
            #[["id", "title", "type", "publicationYear","publisher", "venue_type", "publication_venue"]]
            
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
            book_df = PublicationsDF.query("venue_type == 'book'")
            book_df = book_df[["publication_venue"]]
            self.Book_DF = book_df.drop_duplicates(subset=["publication_venue"])
                
            
            #JOURNAL DATAFRAME
            journal_df = PublicationsDF.query("venue_type == 'journal'")
            journal_df= journal_df[["publication_venue"]]
            self.Journal_DF = journal_df.drop_duplicates(subset=["publication_venue"])
           
            #PROCEEDINGS DATAFRAME
            proceedings_df= PublicationsDF.query("venue_type == 'proceedings'")
            proceedings_df = proceedings_df[["publication_venue", "event"]]
            self.Proceedings_DF = proceedings_df.drop_duplicates(subset=["publication_venue"])

        else:
            print("WARNING: CSV file '" + csv + "' does not exist!")

    
    
class DataJSON(object):
    def __init__(self, jsn):
        #self.jsn = jsn
       
        if os.path.exists(jsn):
        # read JSON 
            with open(jsn, "r", encoding="utf-8") as f:
                json_doc = load(f)

            # DATAFRAME FROM JSON 
        
            #VENUE DATAFRAME
            venues_df = json_doc["venues_id"]
            self.VenuesId_DF = pd.DataFrame(venues_df.items(), columns=['doi', 'issn_isbn']).explode('issn_isbn')
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
              
            
            #VENUE EXT DATAFRAME  
            venues_df = json_doc["venues_id"]
            venues_df = pd.DataFrame(venues_df.items(), columns=['doi', 'issn_isbn']).explode('issn_isbn')  
            venues_df = venues_df[["issn_isbn"]].drop_duplicates(subset=["issn_isbn"])
            venues_df.rename(columns={"issn_isbn":"id"}, inplace = True)
            self.VenuesEXT_DF = venues_df
=======
                   
>>>>>>> Stashed changes
=======
                   
>>>>>>> Stashed changes
=======
                   
>>>>>>> Stashed changes
=======
                   
>>>>>>> Stashed changes
=======
                   
>>>>>>> Stashed changes
        
            #AUTHOR DATAFRAME
            author = json_doc["authors"]
            author_df=DataFrame(author.items(),columns=['doi','author']).explode('author')
            author_df = pd.json_normalize(json.loads(author_df.to_json(orient="records")))
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
            #print(self.Cites_DF)

            #PERSON DATAFRAME 
            author = json_doc["authors"]
            person_df=pd.DataFrame(author.items(),columns=['doi','author']).explode('author')
            person_df=pd.json_normalize(json.loads(person_df.to_json(orient="records")))
            person_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
            person_df.drop("doi", axis =1, inplace = True)
            self.Person_DF = person_df
            

            #ORGANIZATION DATAFRAME 
            crossref = json_doc["publishers"]
            id_and_name = crossref.values()
            organization_df = pd.DataFrame(id_and_name)
            self.Organization_DF = organization_df 

        else:
            print("WARNING: JSON file '" + jsn + "' does not exist!")

p = "./relational_db/relational_other_data.json"
csv= "./relational_db/relational_publication.csv"
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
Dataobject = DataJSON(p)
#print(Dataobject.VenuesEXT_DF)
=======
#Dataobject = DataCSV(csv)
#print(Dataobject.Journal_DF)
>>>>>>> Stashed changes
=======
#Dataobject = DataCSV(csv)
#print(Dataobject.Journal_DF)
>>>>>>> Stashed changes
=======
#Dataobject = DataCSV(csv)
#print(Dataobject.Journal_DF)
>>>>>>> Stashed changes
=======
#Dataobject = DataCSV(csv)
#print(Dataobject.Journal_DF)
>>>>>>> Stashed changes
=======
#Dataobject = DataCSV(csv)
#print(Dataobject.Journal_DF)
>>>>>>> Stashed changes
