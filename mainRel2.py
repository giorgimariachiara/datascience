from locale import normalize
# read csv file with pandas

from implRel import RelationalProcessor
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
from pandas import DataFrame, concat
from extraclasses import Data

csv = "./relational_db/relational_publication.csv"
jsn = "./relational_db/relational_other_data.json"

class RelationalDataProcessor(RelationalProcessor):    
    def uploadData(self, publication_DB):

        Dataobject = Data(csv, jsn)
        
        
        #Populate the SQL database 
        with connect(publication_DB) as con:
            Dataobject.Venue_DF.to_sql("Venue", con, if_exists="replace", index=False)
            Dataobject.Journal_DF.to_sql("Journal", con, if_exists="replace", index=False)
            Dataobject.Book_DF.to_sql("Book", con, if_exists="replace", index=False)
            Dataobject.Journal_article_DF.to_sql("JournalArticle", con, if_exists="replace", index=False)
            Dataobject.Book_chapter_DF.to_sql("BookChapter", con, if_exists="replace", index=False)
            Dataobject.Proceedings_DF.to_sql("Proceedings", con, if_exists="replace", index=False)
            Dataobject.Organization_DF.to_sql("Organization", con, if_exists="replace", index=False)
            Dataobject.Person_DF.to_sql("Person", con, if_exists="replace", index=False)
            Dataobject.Author_DF.to_sql("Authors", con, if_exists="replace", index=False)
            Dataobject.Cites_DF.to_sql("Cites", con, if_exists="replace", index=False)
            Dataobject.Proceedings_paper_DF.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)  
            Dataobject.VenuesExt_DF.to_sql("VenueExt", con, if_exists="replace", index=False)  
            Dataobject.Publication_DF.to_sql("Publications", con, if_exists="replace", index=False)  


            con.execute("DROP VIEW  IF EXISTS countCited") 
            con.execute("CREATE VIEW countCited AS "
                        "SELECT cited, count(*) AS N FROM Cites GROUP BY cited HAVING cited IS NOT NULL;")
            con.execute("DROP VIEW  IF EXISTS maxCited") 
            con.execute("CREATE VIEW maxCited AS "
                        "SELECT * FROM countCited WHERE N = (SELECT MAX(N) FROM countCited);")
                        

            con.commit()

runner = RelationalDataProcessor()

print(runner.uploadData("publication.db"))