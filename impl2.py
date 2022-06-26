from posixpath import split
import sqlite3
from sqlite3 import * 
from pandas import DataFrame, concat, read_sql
import pandas



# from mimetypes import init
# from unicodedata import name
# from impl import IdentifiableEntity


# class Pubblication(IdentifiableEntity):
#     def __init__(self, id, publication_year, title, publicationVenue, cites, author):
#         super().__init__(id)
#         self.publication_year = publication_year
#         self.title = title
#         self.PublicationVenue = publicationVenue
#         self.cites = cites
#         self.author = author
        
        
#     def getPublicationYear(self):
#         if self.publication_year:
#             return self.publication_year

#     def getTitle(self):
#         return self.title

#     def getCitedPublications(self):
#         resultList = []
#         for citedPublication in self.cites:
#             resultList.append(citedPublication)
#         return resultList    

#     def getPublicationVenue(self):
#         return self.getPublicationVenue

#     def getAuthors(self):
#         resultSet = set()
#         for person in self.author:
#             resultSet.add(person)
#         return resultSet

# class Person(IdentifiableEntity):
#     def __init__(self, id, givenName, familyName):
#             super().__init__(id)
#             self.givenName = givenName
#             self.familyName = familyName
            

#     def getGivenName(self):
#             return self.givenName   

#     def getFamilyName(self):
#             return self.familyName   


# class IdentifiableEntity(object):
#     def __init__(self, id):  
#             self.id = id 

#     def getIds(self):
#         resultList = []       
#         for idx in id:
#                 resultList.append(idx)
#         return(resultList)   

# class Venue(IdentifiableEntity):
#     def __init__(self, id, title, publisher):
#         super().__init__(id) 
#         self.title = title 
#         self.publisher = publisher 

#     def getTitle(self):
#         return self.title

#     def getPublisher(self):
#         return self.publisher

# class Organization(IdentifiableEntity):
#     def __init__(self, id, name):
#          super().__init__(id)
#          self.name = name

#     def getName(self):
#         return self.name


# class JournalArticle(Pubblication):
#     def __init__(self, id, publication_year, title, publicationVenue, cites, author, issue, volume):
#         super().__init__(id, publication_year, title, publicationVenue, cites, author)     
#         self.issue = issue
#         self.volume = volume

#     def getIssue(self):
#         if self.issue:
#             return self.issue
#         else:
#             return None

#     def getVolume(self):
#         if self.volume:
#             return self.volume
#         else:
#             return None 

# class BookChapter(Pubblication):
#     def __init__(self, id, publication_year, title, publicationVenue, cites, author, chapterNumber):
#         super().__init__(id, publication_year, title, publicationVenue, cites, author) 
#         self.chapterNumber = chapterNumber

#     def getChapterNumber(self):
#         return self.chapterNumber

# class ProceedingsPaper(Pubblication):
#     def __init__(self, id, publication_year, title, publicationVenue, cites, author):
#          super().__init__(id, publication_year, title, publicationVenue, cites, author)

# class Journal(Venue):
#     def __init__(self, id, title, publisher):
#          super().__init__(id, title, publisher)



# class Book(Venue):
#     def __init__(self, id, title, publisher):
#          super().__init__(id, title, publisher)

# class Proceedings(Venue):
#     def __init__(self, id, title, publisher, event):
#         super().__init__(id, title, publisher) 
#         self.event = event  

#     def getEvent(self):
#         return self.event


dbPath = "/home/ljutach/Desktop/DHDK_magistrale/courses/DataScience/FinalProject/GitRep/datascience/publications.db"

from mimetypes import init
from tokenize import String


class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = [] # : QueryProcessor[0..*]

    def cleanQueryProcessors(self):
        if len(self.queryProcessor) != 0:
                self.queryProcessor.clear()
        return True

    def addQueryProcessor(self, QueryProcessor):
        self.queryProcessor.append(QueryProcessor)
        return True

    def getQueryProcessorElement(self, elementNumber):
        return self.queryProcessor[elementNumber]

    def getPublicationsPublishedInYear(self, publicationYear):
        print("jhjhgjg" + str(publicationYear))
        rqp0 = RelationalQueryProcessor()
        #xyz = RelationalQueryProcessor.getPublicationsPublishedInYear(publicationYear)
        dfPY = rqp0.getPublicationsPublishedInYear(publicationYear)
        self.addQueryProcessor(dfPY)
        return self.queryProcessor
    
    def getPublicationsByAuthorId(self, orcid):
        rqp0 = RelationalQueryProcessor()
        dfAuthor = rqp0.getPublicationsByAuthorId(orcid)
        self.addQueryProcessor(dfAuthor)
        return self.queryProcessor
    
    def getPublicationInVenue(self, publication):
        rqp0 = RelationalQueryProcessor()
        dfPV = rqp0.getPubicationInVenue(publication)
        self.addQueryProcessor(dfPV)
        return self.queryProcessor

    def getPublicationAuthors(self, publication):
        rqp0 = RelationalQueryProcessor()
        dfAP =rqp0.getPublicationAuthors(publication)
        self.addQueryProcessor(dfAP)
        return self.queryProcessor

    def getVenuesByPublisherId(self, publisher):
        rqp0 = RelationalQueryProcessor()
        dfVP = rqp0.getVenuesByPublisherId(publisher)
        self.addQueryProcessor(dfVP)
        return self.queryProcessor

    def getJournalArticlesInJournal(self, issn):
        rqp0 = RelationalQueryProcessor()
        dfJAJ = rqp0.getJournalArticlesInJournal(issn)
        self.addQueryProcessor(dfJAJ)
        return self.queryProcessor
    
    def getPublicationsByAuthorName(self, name):
        rqp0 = RelationalQueryProcessor()
        dfAN = rqp0.getPublicationsByAuthorName(name)
        self.addQueryProcessor(dfAN)
        return self.queryProcessor

    def getDistinctPublisherOfPublications(self, list):
        rqp0 = RelationalQueryProcessor()
        dfPP = rqp0.getDistinctPublisherOfPublications(list)
        self.addQueryProcessor(dfPP)
        return self.queryProcessor

dbPath = "/home/ljutach/Desktop/DHDK_magistrale/courses/DataScience/FinalProject/GitRep/datascience/publications.db"
#dbPath = "./publications.db" 
#dbPath = "./publication.db"
#dbPath = "./publications.db"

class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""

    def setDbPath(self, path):
        self.dbPath = path
        return True

    def getDbPath(self):
        return self.dbPath

class QueryProcessor(object):
    def __init__(self):
        pass
    


class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):

    def getPublicationsPublishedInYear(self, py):
       rp0 = RelationalProcessor()
       rp0.setDbPath(dbPath)
       print("--->" + rp0.getDbPath())
       with connect(rp0.getDbPath()) as con:   
            con.commit()
           
            JournalArticleDF = read_sql("SELECT * FROM JournalArticle WHERE publication_year = " + str(py), con)
            BookChapterDF = read_sql("SELECT * FROM BookChapter WHERE publication_year = " + str(py), con)
            ProceedingsPaperDF = read_sql("SELECT * FROM ProceedingsPaper WHERE publication_year = " + str(py), con)
            #BookChapterDF = read_sql("SELECT * FROM BookChapter WHERE publication_year = " + str(py) + " LIMIT 2 ", con)
       return concat([JournalArticleDF, BookChapterDF, ProceedingsPaperDF])   


    def getPublicationsByAuthorId(self, orcid):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)   
        with connect(rp0.getDbPath()) as con:   
            #con.commit()
            #JournalArticleDF = read_sql("SELECT A.* FROM JournalArticle AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = '" + orcid + "'", con)
            #BookChapterDF = read_sql("SELECT * FROM BookChapter AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = " + str(orcid), con)  
            #ProceedingsPaperDF = read_sql("SELECT * FROM ProceedingsPaper AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = " + str(orcid), con)
            publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            SQL = "SELECT A.* FROM {} AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = '{}'"
            return concat([
                read_sql(SQL.format(publications[0], orcid), con),
                read_sql(SQL.format(publications[1], orcid), con),
                read_sql(SQL.format(publications[2], orcid), con)
            ])
            

    def getPublicationAuthors(self, publication): #QUI HO CAMBIATO OUTPUT
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            #con.commit()
            publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            SQL = "SELECT C.* FROM {} AS A JOIN Authors AS B ON A.doi == B.doi JOIN Person AS C ON B.orc_id == C.orcid WHERE A.doi = '{}'"
            return concat([
                read_sql(SQL.format(publications[0], publication), con),
                read_sql(SQL.format(publications[1], publication), con),
                read_sql(SQL.format(publications[2], publication), con)
            ])
            #JournalArticleDF = read_sql("SELECT C.* FROM JournalArticle AS A JOIN Authors AS B ON A.doi == B.doi JOIN Person AS C ON B.orc_id == C.orcid WHERE A.doi = '" + publication + "'", con)

        #return JournalArticleDF
    

    def getVenuesByPublisherId(self, publisher): #ho messo drop duplicates così leva i duplicati ma secondo me non serve la colonna issn/isbn
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            con.commit()
            VenuesDF = read_sql("SELECT * FROM Venueid WHERE publisher = '" + publisher + "'", con)
        return VenuesDF.drop_duplicates(subset=['publication_venue'])
    
    def getJournalArticlesInJournal(self, issn):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            con.commit()
        
            JournalArticles = read_sql("SELECT * FROM JournalArticle LEFT JOIN Venueid ON JournalArticle.doi == Venueid.id WHERE issn_isbn = " + "'issn'", con) 
        return JournalArticles

    def getPublicationsByAuthorName(self, name):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con:
            #con.commit()
            #Authorsname = read_sql("SELECT * FROM JournalArticle WHERE doi='doi:10.1080/08989621.2020.1836620'", con)
            #print(type(Authorsname))
            SQL = 'SELECT A.* FROM BookChapter A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given LIKE "%' + name + '%"'
            print(SQL)
            Authorsname = read_sql(SQL, con)  
        return(Authorsname) 

    # def getDistinctPublisherOfPublications(self, list):
    #     rp0= RelationalProcessor()
    #     rp0.setDbPath(dbPath)
    #     list = tuple(list)
    #     with connect(rp0.getDbPath()) as con:
    #         PublishP = read_sql("SELECT A.* FROM Organization AS A JOIN Venueid AS B ON A.id == B.id WHERE A.id= '" + str(list) + "'", con) 

    #     return PublishP
    
    def getDistinctPublisherOfPublications(self, list):
        rp0= RelationalProcessor()
        rp0.setDbPath(dbPath)
        publisherDFlist = []
        with connect(rp0.getDbPath()) as con:
            con.commit()
            for doi in list:
                publisherDF = read_sql("SELECT DISTINCT A.* FROM Organization A JOIN Venueid B ON A.id == B.publisher WHERE B.id =" + "'" + doi + "'", con)
                publisherDFlist.append(publisherDF)
        return concat(publisherDFlist)        
             


    
    
    
    
    
"""
    def getPublicationInVenue(self, publication):
        rp0= RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 


SQL = "SELECT A.* FROM {} A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given = '%{}%'"
            #D.given LIKE "%' + {} + '%
            return concat([
                read_sql(SQL.format(publications[0], name), con),
                read_sql(SQL.format(publications[1], name), con),
                read_sql(SQL.format(publications[2], name), con)
            ])
"""

#def pathSetter():

  
testList = ["doi:10.1007/s11192-019-03217-6", "doi:10.1162/qss_a_00023"]


rqp = RelationalQueryProcessor()
#gqp = GenericQueryProcessor()
# gqp.getPublicationsPublishedInYear(2020)
# print(gqp.queryProcessor)

# gqp.addQueryProcessor("wefwef")
# gqp.addQueryProcessor("qwewqef23r")
# gqp.addQueryProcessor(["dw", 3])


# print(gqp.getQueryProcessorElement(2))
# gqp.cleanQueryProcessors()
# print(gqp.getQueryProcessorElement(0))


#print(rqp.getDbPath())
#print(RelationalProcessor.getDbPath())

#rqp.setDbPath(dbPath)
#rqp.setDbPath(dbPath)  
#print(rqp.getPublicationsPublishedInYear(2020))
#print(rqp.getDbPath())
#RelationalQueryProcessor.setDbPath(dbPath)
#print(RelationalQueryProcessor.getDbPath())
#print(gqp.getPublicationsByAuthorId("0000-0001-8686-0017"))

#print(gqp.getPublicationAuthors("doi:10.1162/qss_a_00023"))
#print(gqp.getVenuesByPublisherId(publisher="crossref:281"))

#print(gqp.getPublicationAuthors("doi:10.1007/s11192-019-03217-6"))
#print(gqp.getJournalArticlesInJournal("issn:2641-3337"))
#print(gqp.getVenuesByPublisherId("crossref:281"))
#print(gqp.getPublicationsByAuthorName("Pe"))
#print(gqp.getDistinctPublisherOfPublications(["doi:10.1007/s11192-019-03217-6"]))
print(rqp.getDistinctPublisherOfPublications(testList))
