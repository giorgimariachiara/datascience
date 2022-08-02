from importlib.resources import path
import pandas as pd
from posixpath import split
import sqlite3
from sqlite3 import *
from pandas import DataFrame, concat, read_sql
from mimetypes import init
from unicodedata import name
import json
from json import load
from mimetypes import init
from tokenize import String
import os
from extraclasses import DataCSV, DataJSON


#object classes -----------------------------------------------------------------------------------------------------------------------#

class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result


class Publication(IdentifiableEntity):
    def __init__(self, id, publication_year, title, publicationVenue):

        self.publication_year = publication_year
        self.title = title
        self.PublicationVenue = publicationVenue
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.publication_year, self.title, self.PublicationVenue])

    def getPublicationYear(self):
        if self.publication_year:
            return self.publication_year

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.getPublicationVenue


class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):

        self.givenName = givenName
        self.familyName = familyName
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.givenName, self.familyName])

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class Venue(IdentifiableEntity):  # issn_isbn is id
    def __init__(self, id, publication_venue, publisher):  # issn_isbn self.issn_isbn = issn_isbn
        self.publisher = publisher
        self.publication_venue = publication_venue
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.publication_venue, self.publisher])

    def getPublicationVenue(self):
        return self.publication_venue

    def getPublisher(self):
        return self.publisher


class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.name])

    def __str__(self):
        return str([self.id, self.name])

    def getName(self):
        return self.name


class JournalArticle(Publication):
    def __init__(self, id, publication_year, title, publication_venue, issue, volume):
        self.publication_venue = publication_venue
        self.issue = issue
        self.volume = volume
        super().__init__(id, publication_year, title, publication_venue)

    def __str__(self):
        return str([self.id, self.publication_year, self.title, self.publication_venue, self.issue, self.volume])

    def getIssue(self):
        if self.issue:
            return self.issue
        else:
            return None

    def getVolume(self):
        if self.volume:
            return self.volume
        else:
            return None


class BookChapter(Publication):
    def __init__(self, id, publication_year, title, publicationVenue, cites, author, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, publication_year, title, publicationVenue, cites, author)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    def __init__(self, id, publication_year, title, publicationVenue, cites, author):
        super().__init__(id, publication_year, title, publicationVenue, cites, author)


class Journal(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)


class Book(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)


class Proceedings(Venue):
    def __init__(self, id, publication_venue, publisher, event):
        self.event = event
        super().__init__(id, publication_venue, publisher)

    def __str__(self):
        return str([self.id, self.publication_venue, self.publisher, self.event])

    def getEvent(self):
        return self.event


#---------------------------------------------------------------------------------------------------------------------------------------------#


class QueryProcessor(object):
    def __init__(self):
        pass



# CLASSES FOR RDF DATABASE---------------------------------------------------------------------------------------------------------------------#


class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def setEndpointUrl(self, url):
        self.endpointUrl = url

    def getEndpointUrl(self):
        return self.endpointUrl


#  CLASSES FOR RELATIONAL DATABASE --------------------------------------------------------------------------------------------------------------#


class RelationalProcessor(object):
    def __init__(self, dbPath=""):

        self.dbPath = dbPath

    def setDbPath(self, dbPath):
        self.dbPath = dbPath
        return True

    def getDbPath(self):
        return self.dbPath


class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, dbPath=""):
        super().__init__(dbPath)

    def uploadData(self, path):  # path to input data file
        print("dbpath =" + self.getDbPath())
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            # la classe data verrà divisa in due classi DataCSV e DataJson
            CSV_Rdata = DataCSV(path)
#           CSV_Rdata2SQLite.Rdata2SQLite(CSV_Rdata, .getDbPath())
            with connect(self.getDbPath()) as con:
                CSV_Rdata.Book_DF.to_sql(
                    "Book", con, if_exists="replace", index=False)
                CSV_Rdata.Publication_DF.to_sql(
                    "Publications", con, if_exists="replace", index=False)
                CSV_Rdata.Journal_DF.to_sql(
                    "Journal", con, if_exists="replace", index=False)
                CSV_Rdata.Proceedings_DF.to_sql(
                    "Proceeding", con, if_exists="replace", index=False)
                CSV_Rdata.Proceedings_paper_DF.to_sql(
                    "ProceedingPaper", con, if_exists="replace", index=False)
                CSV_Rdata.Journal_article_DF.to_sql(
                    "JournalArticle", con, if_exists="replace", index=False)
                CSV_Rdata.Book_chapter_DF.to_sql(
                    "BookChapter", con, if_exists="replace", index=False)
                

                con.commit()

        elif f_ext.upper() == ".JSON":
            JSN_Rdata = DataJSON(path)
            with connect(self.getDbPath()) as con:
                #            JSN_Rdata2SQLite(JSN_Rdata, .getDbPath())
                JSN_Rdata.Author_DF.to_sql(
                    "Authors", con, if_exists="replace", index=False)
                JSN_Rdata.Cites_DF.to_sql(
                    "Cites", con, if_exists="replace", index=False)
                JSN_Rdata.Organization_DF.to_sql(
                    "Organization", con, if_exists="replace", index=False)
                JSN_Rdata.VenuesId_DF.to_sql(
                    "Venue", con, if_exists="replace", index=False)
                JSN_Rdata.Person_DF.to_sql(
                    "Person", con, if_exists="replace", index=False)
                JSN_Rdata.VenueEXT_DF.to_sql(
                    "VenueEXT", con, if_exists="replace", index= False)

                
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


#  GENERIC QUERY PROCESSOR ---------------------------------------------------------------------------------------------------------------#


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = []  # : QueryProcessor[0..*]

    def cleanQueryProcessors(self):
        if len(self.queryProcessor) != 0:
            self.queryProcessor.clear()
        return True

    def addQueryProcessor(self, QueryProcessor):
        self.queryProcessor.append(QueryProcessor)
        return True

    def getPublicationsPublishedInYear(self, publicationYear):
        res = []
        for QP in self.queryProcessor:
            re = QP.getPublicationsPublishedInYear(publicationYear)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                publicationObj = Publication(*row)
                result.append(publicationObj)

        return result

    def getMostCitedPublication(self):
        res = []
        for QP in self.queryProcessor:
            re = QP.getMostCitedPublication()
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                publicationObj = Publication(*row)
                result.append(publicationObj)

        return result

    def getPublicationsByAuthorId(self, orcid):
        res = []
        for QP in self.queryProcessor:
            re = QP.getPublicationsByAuthorId(orcid)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                publicationObj = Publication(*row)
                result.append(publicationObj)

        return result
           
    def getMostCitedVenue(self):
        res = []
        for QP in self.queryProcessor:
            re = QP.getMostCitedVenue()
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                VenueObj = Venue(*row)
                result.append(VenueObj)
        return result

    def getVenuesByPublisherId(self, publisher):
        res = []
        for QP in self.queryProcessor:
            re = QP.getVenuesByPublisherId(publisher)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                Venueobj = Venue(*row)
                result.append(Venueobj)

        return result

    def getPublicationInVenue(self, publication):
        rqp0 = RelationalQueryProcessor()
        dfPV = rqp0.getPublicationInVenue(publication)
        for index, row in dfPV.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor

    def getJournalArticlesInIssue(self, volume, issue, issn_isbn):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getJournalArticlesInIssue(volume, issue, issn_isbn)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                JournalarticleObj = JournalArticle(*row)
                result.append(JournalarticleObj)
        return result
      
    def getJournalArticlesInVolume(self, volume, issn_isbn):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getJournalArticlesInVolume(volume, issn_isbn)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                JournalarticleObj = JournalArticle(*row)
                result.append(JournalarticleObj)
        return result

    def getJournalArticlesInJournal(self, issn):
        res = []
        for QP in self.queryProcessor:
            re = QP.getJournalArticlesInJournal(issn)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                JournalArticleOBJ = JournalArticle(*row)
                result.append(JournalArticleOBJ)

        return result
              
    def getProceedingsByEvent(self, name):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getProceedingsByEvent(name)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                ProceedingObj = Proceedings(*row)
                result.append(ProceedingObj)
        return result

    def getPublicationAuthors(self, publication):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getPublicationAuthors(publication)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                personObj = Person(*row)
                result.append(personObj)

        return result
    
    def getPublicationsByAuthorName(self, name):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getPublicationsByAuthorName(name)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                publicationObj = Publication(*row)
                result.append(publicationObj)
        return result

    def getDistinctPublisherOfPublications(self, lista):
        res = []
        for QP in self.queryProcessor: 
            re = QP.getDistinctPublisherOfPublications(lista)
            res.append(re)
        result = []
        for el in res:
            for index, row in el.iterrows():
                row = list(row)
                OrganizationObj = Organization(*row)
                result.append(OrganizationObj)
        return result



# RELATIONAL QUERY PROCESSOR ----------------------------------------------------------------------------------------------------------#


class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, publicationYear):
        with connect(self.getDbPath()) as con:
            #SQL = "SELECT id, publication_year, title, publication_venue FROM Publications WHERE publicationYear = '{}'"
            SQL = "SELECT id, publicationYear, title, publication_venue FROM Publications WHERE publicationYear = " + \
                str(publicationYear) + ";"
            return read_sql(SQL, con)

    def getPublicationsByAuthorId(self, orcid):  
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.id, A.publicationYear, A.title, A.publication_venue FROM Publications AS A JOIN Authors AS B ON A.id == B.doi WHERE B.orc_id = '" + orcid + "';"
            return read_sql(SQL, con)

    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            SQL = "SELECT id, publicationYear, title, publication_venue " \
                "FROM Publications JOIN maxCited ON id = cited"
            return read_sql(SQL, con)

    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.issn_isbn, B.publication_venue, B.publisher FROM Venue AS A JOIN Publications AS B ON A.doi == B.id \
                                JOIN maxCited ON id == cited"
        return read_sql(SQL, con) 

    def getVenuesByPublisherId(self, publisher):
        with connect(self.getDbPath()) as con:
            SQL = read_sql("SELECT C.issn_isbn, A.name, B.publication_venue FROM Organization AS A JOIN Publications AS B ON A.id == B.publisher LEFT JOIN Venue AS C ON B.id == C.doi WHERE A.id = '" + publisher + "'", con)
        return SQL.drop_duplicates(subset=['publication_venue'])  
 
    def getPublicationInVenue(self, issn_isbn):
        with connect(self.getDbPath()) as con:
            SQL ="SELECT A.id, A.publicationYear, A.title, A.publication_venue FROM Publications AS A JOIN Venue AS B ON A.id == B.doi WHERE B.issn_isbn = '" + issn_isbn + "'"
            return read_sql(SQL, con)

    def getJournalArticlesInIssue(self, issue, volume, issn_isbn): 
        with connect(self.getDbPath()) as con: 
            SQL ="SELECT A.id, A.publicationYear, A.title, A.publication_venue, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi WHERE  B.issue = '"+ str(issue) + "' AND B.volume = '" + str(volume) + "' AND C.issn_isbn = '"+ issn_isbn + "'"
        return read_sql(SQL, con) 

    def getJournalArticlesInVolume(self, volume, issn_isbn): 
        with connect(self.getDbPath()) as con: 
            SQL ="SELECT A.id, A.publicationYear, A.title, A.publication_venue, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi WHERE B.volume = '" + str(volume) + "' AND C.issn_isbn = '"+ issn_isbn + "'"
        return read_sql(SQL, con) 

    def getJournalArticlesInJournal(self, issn):
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.id, A.publicationYear, A.title, A.publication_venue, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi  WHERE C.issn_isbn = '" + issn + "'"
        return read_sql(SQL, con)

    def getPublicationAuthors(self, publication):
        with connect(self.getDbPath()) as con: 
            SQL = "SELECT orc_id, family_name, given_name FROM Person WHERE doi = '" + publication + "';"
        return read_sql(SQL, con)

    def getPublicationsByAuthorName(self, name): 
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.id, A.publicationYear, A.title, A.publication_venue FROM Publications AS A JOIN Person AS B ON A.id == B.doi WHERE B.given_name LIKE '%" + name + "%'"
        return read_sql(SQL, con) 
    
    def getProceedingsByEvent(self, eventPartialName): 
        with connect(self.getDbPath()) as con:
            eventPartialName.lower()
            SQL = "SELECT A.publication_venue, B.publisher, A.event FROM Proceeding AS A JOIN Publications AS B ON A.publication_venue == B.publication_venue WHERE A.event == '%" + eventPartialName + "%'"
        return read_sql(SQL, con)
    
    def getDistinctPublisherOfPublications(self, listOfDoi):
        with connect(self.getDbPath()) as con:
            publisherDF = pd.DataFrame()
            for doi in listOfDoi:
                SQL = read_sql("SELECT A.id, A.name FROM Organization AS A JOIN Publications AS B ON A.id == B.publisher WHERE B.id = '" + doi + "'", con)
                publisherDF = concat([publisherDF, SQL]) 
        return  publisherDF