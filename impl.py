from importlib.resources import path
from logging import raiseExceptions
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
import os
from extraclasses import DataCSV, DataJSON
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


# OBJECT CLASSES -----------------------------------------------------------------------------------------------------------------------#

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


class Venue(IdentifiableEntity):
    def __init__(self, id, publication_venue, publisher): 
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
    def __init__(self, id, publication_year, title, publicationVenue, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, publication_year, title, publicationVenue)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    def __init__(self, id, publication_year, title, publicationVenue):
        super().__init__(id, publication_year, title, publicationVenue)


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


# QUERY PROCESSOR----------------------------------------------------------------------------------------------------------------------------#


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


class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            CSV_Rdata = DataCSV(path)

            base_url = "https://github.com/giorgimariachiara/datascience/res/"

            JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
            BookChapter = URIRef("https://schema.org/Chapter")
            Proceedingspaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
            Journal = URIRef("https://schema.org/Periodical")
            Book = URIRef("https://schema.org/Book")
            Proceeding = URIRef("http://purl.org/ontology/bibo/Proceedings") 
            Publication = URIRef("https://schema.org/CreativeWork")


            publicationYear = URIRef("https://schema.org/datePublished")
            title = URIRef("https://schema.org/name")
            issue = URIRef("https://schema.org/issueNumber")
            volume = URIRef("https://schema.org/volumeNumber")
            identifier = URIRef("https://schema.org/identifier")
            chapter = URIRef("https://schema.org/Chapter")
            event = URIRef("https://schema.org/event") 
            publisher = URIRef("https://schema.org/publisher")
            name = URIRef("https://schema.org/name")

            publicationVenue = URIRef("https://schema.org/isPartOf")

            my_graph = Graph()

            for idx, row in CSV_Rdata.Publications_DF.iterrows():  
                subj = URIRef(base_url + row["id"]) 

                my_graph.add((subj, RDF.type, Publication))
                if row["title"] != "":
                    my_graph.add((subj, title, Literal(row["title"])))
                if row["id"] != "":    
                    my_graph.add((subj, identifier, Literal(row["id"])))
                if row["publicationYear"] != "":
                    my_graph.add((subj, publicationYear, Literal(row["publicationYear"])))
                if row["publicationVenueId"] != "":
                    my_graph.add((subj, publicationVenue, URIRef(base_url + row["publicationVenueId"])))

                if row["type"] == "journal-article":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, JournalArticle)) 
                elif row["type"] == "book-chapter":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, BookChapter))
                elif row["type"]== "proceedings-paper":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, Proceedingspaper)) 
                else: 
                    print("WARNING: Unrecognized publication type!")
            
            for idx, row in CSV_Rdata.Venue_DF.iterrows():
                subj = URIRef(base_url + row["id"])

                if row["title"] != "":
                    my_graph.add((subj, name, Literal(row["title"])))
                if row["id"] != "":
                    my_graph.add((subj, identifier, Literal(row["id"])))
                if row["publisherId"] != "":
                    my_graph.add((subj, publisher, Literal(row["publisherId"])))

                     
            #triple publications type

            for idx, row in CSV_Rdata.Journal_article_DF.iterrows():
                subj = URIRef(base_url + row["id"])
 
                if row["issue"] != "":
                    my_graph.add((subj, issue, Literal(row["issue"])))
                if row["volume"] != "":
                    my_graph.add((subj, volume, Literal(row["volume"])))
            
            for idx, row in CSV_Rdata.Book_chapter_DF.iterrows():
                subj = URIRef(base_url + row["id"])
                
                if row["chapter"] != "": 
                    my_graph.add((subj, chapter, Literal(row["chapter"])))
            
            #triple Venue type 
            
            for idx, row in CSV_Rdata.Book_DF.iterrows():
                local_id = "book-" + str(idx)
                subj = URIRef(base_url + local_id)
                my_graph.add((subj, name, Literal(row["publication_venue"])))
                my_graph.add((subj, RDF.type, Book))

            for idx, row in CSV_Rdata.Journal_DF.iterrows():
                local_id = "journal-" + str(idx)
                subj = URIRef(base_url + local_id)
                my_graph.add((subj, name, Literal(row["publication_venue"])))
                my_graph.add((subj, RDF.type, Journal))
            
            for idx, row in CSV_Rdata.Proceedings_DF.iterrows():
                local_id = "proceeding-" + str(idx)
                subj = URIRef(base_url + local_id) 
                my_graph.add((subj, RDF.type, Proceeding))
                if row["publication_venue"] != "":
                    my_graph.add((subj, name, Literal(row["publication_venue"])))
                if row["event"] != "":  
                    my_graph.add((subj, event, Literal(row["event"])))
                
                
            self.my_graph= my_graph

            store = SPARQLUpdateStore()
            # The URL of the SPARQL endpoint is the same URL of the Blazegraph
            # instance + '/sparql'
            endpointUrl = self.getEndpointUrl()

            # It opens the connection with the SPARQL endpoint instance
            store.open((endpointUrl, endpointUrl))

            for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
                store.add(triple)   
            # Once finished, remeber to close the connection
            store.close()
            
        elif f_ext.upper() == ".JSON":
            JSN_Rdata = DataJSON(path)

            base_url = "https://github.com/giorgimariachiara/datascience/res/"

            Person = URIRef("https://schema.org/Person")
            Organization = URIRef("https://schema.org/Organization")

            identifier = URIRef("https://schema.org/identifier")
            familyName = URIRef("https://schema.org/familyName")
            givenName = URIRef("https://schema.org/givenName") 
            name = URIRef("https://schema.org/name")
            citation = URIRef("https://schema.org/citation")
            author = URIRef("https://schema.org/author")
            issn_isbn = URIRef("http://purl.org/dc/terms/identifier")


            my_graph = Graph()

            for idx, row in JSN_Rdata.Organization_DF.iterrows():
                subj = URIRef(base_url + row["id"])

                my_graph.add((subj, RDF.type, Organization))
                my_graph.add((subj, name, Literal(row["name"])))  
                my_graph.add((subj, identifier, Literal(row["id"])))

            for idx, row in JSN_Rdata.Cites_DF.iterrows():
                subj = URIRef(base_url + row["citing"])

                if row["cited"] != None:
                    my_graph.add((subj, citation, URIRef(base_url + str(row["cited"]))))
            
            for idx, row in JSN_Rdata.VenuesId_DF.iterrows():
                subj = URIRef(base_url + row["doi"])
                
                my_graph.add((subj, issn_isbn, Literal(row["issn_isbn"])))    

            for idx, row in JSN_Rdata.Person_DF.iterrows():
                subjperson = URIRef(base_url + row["orc_id"]) 
            
                my_graph.add((subjperson, RDF.type, Person))
                my_graph.add((subjperson, givenName, Literal(row["given_name"])))
                my_graph.add((subjperson, familyName, Literal(row["family_name"])))
                my_graph.add((subjperson, identifier, Literal(row["orc_id"])))

            for idx, row in JSN_Rdata.Author_DF.iterrows():     
                
                my_graph.add(((URIRef(base_url + row["orc_id"]), author, URIRef(base_url + row["doi"]))))

            self.my_graph = my_graph

            store = SPARQLUpdateStore()
            # The URL of the SPARQL endpoint is the same URL of the Blazegraph
            # instance + '/sparql'
            #endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
            endpointUrl = self.getEndpointUrl()

            # It opens the connection with the SPARQL endpoint instance
            store.open((endpointUrl, endpointUrl))

            for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
                store.add(triple)   
            # Once finished, remeber to close the connection
            store.close()

        else:
            raiseExceptions("Problem: the input file has not neither a .csv nor a .json extension!")
            return False

        return True
        

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
            CSV_Rdata = DataCSV(path)
            with connect(self.getDbPath()) as con:
                CSV_Rdata.Venue_DF.to_sql(
                    "VenueId", con, if_exists="replace", index=False)
                CSV_Rdata.Book_DF.to_sql(
                    "Book", con, if_exists="replace", index=False)
                CSV_Rdata.Publications_DF.to_sql(
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


# RELATIONAL QUERY PROCESSOR ----------------------------------------------------------------------------------------------------------#


class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, publicationYear):
        if type(publicationYear) == int:
            with connect(self.getDbPath()) as con:
                SQL = "SELECT A.id, A.publicationYear, A.title, B.title FROM Publications AS A LEFT JOIN VenueId AS B ON A.PublicationVenueId == B.id WHERE publicationYear = " + \
                    str(publicationYear) + ";"
                return read_sql(SQL, con)
        else:
            raiseExceptions("The input parameter publicationYear is not an integer!")

    def getPublicationsByAuthorId(self, orcid):
        if type(orcid) == str:
            with connect(self.getDbPath()) as con:
                SQL = "SELECT A.id, A.publicationYear, A.title, C.title FROM Publications AS A JOIN Authors AS B ON A.id == B.doi JOIN VenueId AS C ON A.PublicationVenueId == C.id WHERE B.orc_id = '" + orcid + "';"
                return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter orcid is not a string!")

    def getMostCitedPublication(self):
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.id, A.publicationYear, A.title, B.title FROM Publications AS A JOIN maxCited AS C ON A.id = C.cited JOIN VenueId AS B ON A.PublicationVenueId == B.id"
            return read_sql(SQL, con)

    def getMostCitedVenue(self):
        with connect(self.getDbPath()) as con:
            SQL = "SELECT A.id, A.title, A.publisherId FROM VenueId AS A JOIN Publications AS B ON B.publicationVenueId == A.id JOIN maxCited ON B.id == cited"
        return read_sql(SQL, con) 

    def getVenuesByPublisherId(self, publisher):
        if type(publisher) == str:
            with connect(self.getDbPath()) as con:
                SQL = read_sql("SELECT B.id, B.title, B.publisherId FROM Organization AS A JOIN VenueId AS B ON B.publisherId == A.id WHERE A.id = '" + publisher + "'", con)
            return SQL.drop_duplicates(subset=['title', 'publisherId'])
        else: 
            raiseExceptions("The input parameter publisher is not a string!")  
 
    def getPublicationInVenue(self, issn_isbn):
        if type(issn_isbn) == str:
            with connect(self.getDbPath()) as con:
                SQL ="SELECT A.id, A.publicationYear, A.title, C.title FROM Publications AS A JOIN Venue AS B ON A.id == B.doi JOIN VenueId AS C ON C.id == A.publicationVenueId WHERE B.issn_isbn = '" + issn_isbn + "'"
                return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter issn_isbn is not a string!")

    def getJournalArticlesInIssue(self, issue, volume, issn_isbn): 
        if type(issue) == str and type(volume) == str and type(issn_isbn) == str:
            with connect(self.getDbPath()) as con: 
                SQL ="SELECT A.id, A.publicationYear, A.title, D.title, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi JOIN VenueId AS D ON D.id == A.publicationVenueId WHERE  B.issue = '"+ str(issue) + "' AND B.volume = '" + str(volume) + "' AND C.issn_isbn = '"+ issn_isbn + "'"
            return read_sql(SQL, con)
        else: 
            raiseExceptions("All or one of the input parameters issue, volume and issn_isbn is not a string!") 
        

    def getJournalArticlesInVolume(self, volume, issn_isbn): 
        if type(volume) == str and type(issn_isbn) == str:
            with connect(self.getDbPath()) as con: 
                SQL ="SELECT A.id, A.publicationYear, A.title, D.title, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi JOIN VenueId AS D ON D.id == A.publicationVenueId WHERE B.volume = '" + str(volume) + "' AND C.issn_isbn = '"+ issn_isbn + "'"
            return read_sql(SQL, con) 
        else: 
            raiseExceptions("All or one of the input parameters volume and issn_isbn is not a string!") 

    def getJournalArticlesInJournal(self, issn):
        if type(issn) == str:
            with connect(self.getDbPath()) as con:
                SQL = "SELECT A.id, A.publicationYear, A.title, D.title, B.issue, B.volume FROM Publications AS A JOIN JournalArticle AS B ON A.id == B.id JOIN Venue AS C ON B.id == C.doi JOIN VenueId AS D ON D.id == A.publicationVenueId WHERE C.issn_isbn = '" + issn + "'"
            return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter issn is not a string!") 

    def getPublicationAuthors(self, publication):
        if type(publication) == str:
            with connect(self.getDbPath()) as con: 
                SQL = "SELECT orc_id, family_name, given_name FROM Person WHERE doi = '" + publication + "';"
            return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter publication is not a string!") 

    def getPublicationsByAuthorName(self, name):
        if type(name) == str: 
            with connect(self.getDbPath()) as con:
                SQL = "SELECT A.id, A.publicationYear, A.title, D.title FROM Publications AS A JOIN Authors AS B ON A.id == B.doi JOIN Person AS C ON C.orc_id == B.orc_id JOIN VenueId AS D ON D.id == A.publicationVenueId WHERE C.given_name LIKE '%" + name + "%'"
            return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter name is not a string!")
        
    
    def getProceedingsByEvent(self, eventPartialName): 
        if type(eventPartialName) == str: 
            with connect(self.getDbPath()) as con:
                eventPartialName.lower()
                SQL = "SELECT A.id, B.title, B.publisherId, C.event FROM Publications AS A JOIN VenueId AS B ON A.publicationVenueId == B.id JOIN Proceeding AS C ON B.title == C.publicationVenueId WHERE C.event == '%" + eventPartialName + "%'"
            return read_sql(SQL, con)
        else: 
            raiseExceptions("The input parameter eventPartialName is not a string!")


    def getDistinctPublisherOfPublications(self, listOfDoi):
        for el in listOfDoi:
            if type(el) == str and type(listOfDoi) == list:
                with connect(self.getDbPath()) as con:
                    publisherDF = pd.DataFrame()
                    for doi in listOfDoi:
                        SQL = read_sql("SELECT A.id, A.name FROM Organization AS A JOIN VenueId AS B ON A.id == B.publisherId JOIN Publications AS C ON B.id == C.publicationVenueId WHERE C.id = '" + doi + "'", con)
                        publisherDF = concat([publisherDF, SQL]) 
                return  publisherDF
            else: 
                raiseExceptions("The input parameter listOfDoi is not a list or one of its elements is not a string!")
            


#  GENERIC QUERY PROCESSOR ---------------------------------------------------------------------------------------------------------------#


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = []  

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
