
from locale import normalize
# read csv file with pandas
from operator import index
from numpy import index_exp
from pandas import merge 
from collections import deque
import json
from json import load, loads
from sqlite3 import connect
from pprint import pprint
from pandas import read_sql
import pandas as pd
from pandas import read_csv, Series, read_json
from pandas import DataFrame, concat
from extraclasses import DataJSON, DataCSV
import os.path

csv = "./relational_db/relational_publication.csv"
jsonf = "./relational_db/relational_other_data.json"
path = "./relational_db/"


class RelationalProcessor(object):
    print("instance of relational processor ")
    def __init__(self, dbPath = ""):
        
        self.dbPath = dbPath

    def setDbPath(self, dbPath):
        self.dbPath = dbPath
        return True

    def getDbPath(self):
        return self.dbPath

class RelationalDataProcessor(RelationalProcessor):  
    def __init__(self, dbPath=""):
        super().__init__(dbPath)
        
    def uploadData(self, path):    # path to input data file
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            CSV_Rdata = DataCSV(path, csv) #la classe data verrà divisa in due classi DataCSV e DataJson 
#           CSV_Rdata2SQLite.Rdata2SQLite(CSV_Rdata, .getDbPath())
            with connect(self.getDbPath()) as con:
                CSV_Rdata.Book_DF.to_sql("Book", con, if_exists="replace", index=False)
                CSV_Rdata.Publication_DF.to_sql("Publications", con, if_exists="replace", index=False)  
                CSV_Rdata.Journal_DF.to_sql("Journal", con, if_exists="replace", index=False) 
                CSV_Rdata.Proceedings_DF.to_sql("Proceeding", con, if_exists="replace", index=False) 
                CSV_Rdata.Proceedings_paper_DF.to_sql("ProceedingPaper", con, if_exists="replace", index=False) 
                CSV_Rdata.Journal_article_DF.to_sql("JournalArticle", con, if_exists="replace", index=False)
                CSV_Rdata.Book_chapter_DF.to_sql("BookChapter", con, if_exists="replace", index=False)

                con.commit()

        elif f_ext.upper() == ".JSON":
            JSN_Rdata = DataJSON(path, jsonf)
            with connect(self.getDbPath()) as con:
#            JSN_Rdata2SQLite(JSN_Rdata, .getDbPath())
                JSN_Rdata.Author_DF.to_sql("Authors", con, if_exists="replace", index=False)
                JSN_Rdata.Cites_DF.to_sql("Cites", con, if_exists="replace", index=False)
                JSN_Rdata.Organization_DF.to_sql("Organization", con, if_exists="replace", index=False)
                JSN_Rdata.Venues_DF.to_sql("Venue", con, if_exists="replace", index=False)
                JSN_Rdata.Person_DF.to_sql("Person", con, if_exists="replace", index=False)

                con.execute("DROP VIEW  IF EXISTS countCited") 
                con.execute("CREATE VIEW countCited AS "
                        "SELECT cited, count(*) AS N FROM Cites GROUP BY cited HAVING cited IS NOT NULL;")
                con.execute("DROP VIEW  IF EXISTS maxCited") 
                con.execute("CREATE VIEW maxCited AS "
                        "SELECT * FROM countCited WHERE N = (SELECT MAX(N) FROM countCited);")

            con.commit()
        else:
            print("problem!!")
            return False

        return True

    """
    def uploadData(self, path):
        
        obj.setDbPath(path) 
        #Populate the SQL database 
        #with connect(RelationalQueryProcessor.getDbPath(self)) as con:
        with connect(obj.getDbPath()) as con:
            
            Dataobject = Data()
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


            con.execute("DROP VIEW  IF EXISTS countCited") 
            con.execute("CREATE VIEW countCited AS "
                        "SELECT cited, count(*) AS N FROM Cites GROUP BY cited HAVING cited IS NOT NULL;")
            con.execute("DROP VIEW  IF EXISTS maxCited") 
            con.execute("CREATE VIEW maxCited AS "
                        "SELECT * FROM countCited WHERE N = (SELECT MAX(N) FROM countCited);")

            con.commit()
            

"""


#print("this module is in name: '" + __name__ + "'")
#if __name__ == "__main__":
    # Dataobject = Data(path, csv, jsn)
    # rpInstance = rp()
    # rpInstance.setDbPath("publication.db")
obj = RelationalDataProcessor() 
obj.setDbPath(path) # primo setting del path al db per caricamento dati
obj.uploadData(csv)
#obj.uploadData(jsn)
    # do the same for triplestore db
#rqp = RelationalQueryProcessor()
#rqp.setDbPath(path) # secondo setting del path al db per le queries
#gqp = GenericQueryProcessor()
#gqp.addQueryProcessor(rqp)
    
#print(gqp.getPublicationsPublishedInYear(2020))