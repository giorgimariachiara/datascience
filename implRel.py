import pandas as pd
from posixpath import split
import sqlite3
from sqlite3 import * 
from pandas import DataFrame, concat, read_sql
import pandas
from mimetypes import init
from unicodedata import name
import json
from json import load


dbPath = "./publication.db"

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

class Venue(IdentifiableEntity): # issn_isbn is id
    def __init__(self, id, publication_venue, publisher): #issn_isbn self.issn_isbn = issn_isbn
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

    def getPublicationsPublishedInYear(self, publicationYear):
        rqp0 = RelationalQueryProcessor()
        dfPY = rqp0.getPublicationsPublishedInYear(publicationYear)
        for index, row in dfPY.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor
    
    def getPublicationsByAuthorId(self, orcid):
        rqp0 = RelationalQueryProcessor()
        dfAuthor = rqp0.getPublicationsByAuthorId(orcid)
        for index, row in dfAuthor.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor


    def getMostCitedPublication(self): 
        rqp0 = RelationalQueryProcessor()
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        dfMCP = rqp0.getMostCitedPublication()
            
        for index, row in dfMCP.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor
    

    # rp0 = RelationalProcessor()
    #     rp0.setDbPath(dbPath)
    #     with connect(rp0.getDbPath()) as con: 
    
    
    """
    def getMostCitedVenue(self):
        rqp0 = RelationalQueryProcessor()
        dfMCV = rqp0.getMostCitedVenue()
        self.addQueryProcessor(dfMCV)
        return self.queryProcessor
    """

    def getVenuesByPublisherId(self, publisher):
        rqp0 = RelationalQueryProcessor()
        dfVP = rqp0.getVenuesByPublisherId(publisher)

        for index, row in dfVP.iterrows():
            row = list(row)
            venueObj = Venue(*row)
            self.addQueryProcessor(venueObj)

        return self.queryProcessor

    def getPublicationInVenue(self, publication):
        rqp0 = RelationalQueryProcessor()
        dfPV = rqp0.getPublicationInVenue(publication)
        for index, row in dfPV.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor
    
    def getJournalArticlesInIssue(self, volume, issue, issn_isbn):
        rqp0 = RelationalQueryProcessor()
        dfJAI = rqp0.getJournalArticlesInIssue(volume, issue, issn_isbn)
        for index, row in dfJAI.iterrows():
            row = list(row)
            JournalarticleObj = JournalArticle(*row)
            self.addQueryProcessor(JournalarticleObj)
        return self.queryProcessor
    
    
    def getJournalArticlesInVolume(self, volume, issn_isbn):
        rqp0 = RelationalQueryProcessor()
        dfJAV = rqp0.getJournalArticlesInVolume(volume, issn_isbn)
        for index, row in dfJAV.iterrows():
            row = list(row)
            JournalarticleObj = JournalArticle(*row)
            self.addQueryProcessor(JournalarticleObj)
        return self.queryProcessor

    def getJournalArticlesInJournal(self, issn):
        rqp0 = RelationalQueryProcessor()
        dfJAJ = rqp0.getJournalArticlesInJournal(issn)
        for index, row in dfJAJ.iterrows():
            row = list(row)
            JournalarticleObj = JournalArticle(*row)
            self.addQueryProcessor(JournalarticleObj)
        return self.queryProcessor

    def getProceedingsByEvent(self, name):
        rqp0 = RelationalQueryProcessor()
        dfPE = rqp0.getProceedingsByEvent(name)
        for index, row in dfPE.iterrows():
            row = list(row)
            ProceedingObj = Proceedings(*row)
            self.addQueryProcessor(ProceedingObj)
        return self.queryProcessor

    def getPublicationAuthors(self, publication):
        rqp0 = RelationalQueryProcessor()
        dfAP =rqp0.getPublicationAuthors(publication)

        for index, row in dfAP.iterrows():
            row = list(row)
            personObj = Person(*row)
            self.addQueryProcessor(personObj)

        return self.queryProcessor
    
    def getPublicationsByAuthorName(self, name):
        rqp0 = RelationalQueryProcessor()
        dfAN = rqp0.getPublicationsByAuthorName(name)
        for index, row in dfAN.iterrows():
            row = list(row)
            publicationObj = Publication(*row)
            self.addQueryProcessor(publicationObj)
        return self.queryProcessor

    def getDistinctPublisherOfPublications(self, lista):
        rqp0 = RelationalQueryProcessor()
        dfPP = rqp0.getDistinctPublisherOfPublications(lista)
        for index, row in dfPP.iterrows():
            row = list(row)
            OrganizationObj = Organization(*row)
            self.addQueryProcessor(OrganizationObj) 
        return self.queryProcessor


class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""

    def setDbPath(self, path):
        self.dbPath = path
        return True

    def getDbPath(self):
        return self.dbPath

class TriplestoreProcessor(object):
    def __init__(self):
        self.endpointUrl = ""
        
    def setEndpointUrl(self, url):
        self.endpointUrl = url
    
    def getEndpointUrl(self):
        return self.endpointUrl
        
        
class QueryProcessor(object):
    def __init__(self):
        pass
    

class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):

    def getPublicationsPublishedInYear(self, py):
       rp0 = RelationalProcessor()
       rp0.setDbPath(dbPath)
       with connect(rp0.getDbPath()) as con:
            
        publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
        SQL = "SELECT doi, publication_year, title, publication_venue FROM {} WHERE publication_year = '{}'"
        return concat([
                read_sql(SQL.format(publications[0], str(py)), con),
                read_sql(SQL.format(publications[1], str(py)), con),
                read_sql(SQL.format(publications[2], str(py)), con)
            ]) 


    def getPublicationsByAuthorId(self, orcid):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)   
        with connect(rp0.getDbPath()) as con:   
            #JournalArticleDF = read_sql("SELECT A.* FROM JournalArticle AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = '" + orcid + "'", con)
            #BookChapterDF = read_sql("SELECT * FROM BookChapter AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = " + str(orcid), con)  
            #ProceedingsPaperDF = read_sql("SELECT * FROM ProceedingsPaper AS A JOIN Authors AS B ON A.doi == B.doi WHERE orc_id = " + str(orcid), con)
            publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            SQL = "SELECT A.doi, A.publication_year, A.title, A.publication_venue FROM {} AS A JOIN Authors AS B ON A.doi == B.doi WHERE B.orc_id = '{}'"
            return concat([
                read_sql(SQL.format(publications[0], orcid), con),
                read_sql(SQL.format(publications[1], orcid), con),
                read_sql(SQL.format(publications[2], orcid), con)
            ])
    
    def getMostCitedPublication(self):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            publicationList = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            sql = "SELECT doi, publication_year, title, publication_venue " \
                    "FROM {} JOIN maxCited ON doi = cited"
            return concat([
                read_sql(sql.format(publicationList[0]), con),
                read_sql(sql.format(publicationList[1]), con),
                read_sql(sql.format(publicationList[2]), con)
            ])
            
            
    def getMostCitedVenue(self):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            venueDF = read_sql("SELECT issn_isbn, publication_venue, publisher FROM Venueid " \
                               "JOIN maxCited ON id == cited", con)
            return venueDF
            
            
    def getVenuesByPublisherId(self, publisher): #ho messo drop duplicates così leva i duplicati ma secondo me non serve la colonna issn/isbn o forse serve ma ne dobbiamo parlare 
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            VenuesDF = read_sql("SELECT A.VenueId, A.publication_venue, A.OrganizationId FROM Venueid AS A JOIN Organization AS B ON A.OrganizationId == B.OrganizationId WHERE B.id = '" + publisher + "'", con)
        return VenuesDF.drop_duplicates(subset=['publication_venue'])

    
    def getPublicationInVenue(self, issn_isbn):
        rp0= RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            SQL ="SELECT A.doi, A.publication_year, A.title, A.publication_venue FROM {} AS A LEFT JOIN VenueExt AS B ON A.publication_venue == B.publication_venue WHERE B.issn_isbn = '{}'"
            return concat([
                read_sql(SQL.format(publications[0], issn_isbn), con),
                read_sql(SQL.format(publications[1], issn_isbn), con),
                read_sql(SQL.format(publications[2], issn_isbn), con)
            ])

     
    
    def getJournalArticlesInIssue(self, volume, issue, issn_isbn): #mi continua a dire str object is not callableee
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            dfJAI = read_sql("SELECT A.doi, A.publication_year, A.title, A.publication_venue, A.issue, A.volume FROM JournalArticle A LEFT JOIN VenueExt B ON A.publication_venue == B.publication_venue WHERE  A.volume = '"+ str(volume) + "' AND A.issue = '" + str(issue) + "' AND B.issn_isbn = '"+ issn_isbn + "'",  con)
        return dfJAI 

    
    
    def getJournalArticlesInVolume(self, volume, issn_isbn): #str object is not callable
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            #dfJAV = read_sql("SELECT title FROM JournalArticle AS A LEFT JOIN Venueid AS B ON A.doi == B.id WHERE volume = {} AND issn_isbn = {})" (volume), (issn_isbn), con)
            dfJAV = read_sql("SELECT A.doi, A.publication_year, A.title, A.publication_venue, A.issue, A.volume FROM JournalArticle AS A LEFT JOIN VenueExt AS B ON A.publication_venue == B.publication_venue WHERE A.volume = '" + str(volume) + "' AND B.issn_isbn = '"+ issn_isbn + "'", con)
        return dfJAV


    def getJournalArticlesInJournal(self, issn):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            dfJAJ = read_sql("SELECT A.doi, A.publication_year, A.title, A.publication_venue, A.issue, A.volume FROM JournalArticle AS A LEFT JOIN VenueExt AS B ON A.publication_venue == B.publication_venue WHERE B.issn_isbn = '" + issn + "'", con) 
        return dfJAJ     
    
    def getProceedingsByEvent(self, name):
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        #name = name.lower()
        with connect(rp0.getDbPath()) as con: 
            events = read_sql('SELECT B.* FROM ProceedingsPaper A LEFT JOIN Proceedings B ON A.doi == B.doi WHERE B.Event LIKE "%' + name.lower() + '%"', con)
        return events


    def getPublicationAuthors(self, publication): 
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con: 
            publications = ["JournalArticle", "BookChapter", "ProceedingsPaper"]
            SQL = "SELECT C.* FROM {} AS A JOIN Authors AS B ON A.doi == B.doi JOIN Person AS C ON B.orc_id == C.orcid WHERE A.doi = '{}'"
            return concat([
                read_sql(SQL.format(publications[0], publication), con),
                read_sql(SQL.format(publications[1], publication), con),
                read_sql(SQL.format(publications[2], publication), con)
            ])
    

    def getPublicationsByAuthorName(self, name): #da controllare come si può mettere il formato più ordinato 
        rp0 = RelationalProcessor()
        rp0.setDbPath(dbPath)
        with connect(rp0.getDbPath()) as con:
        
            dfProc = read_sql('SELECT A.doi, A.publication_year, A.title, A.publication_venue FROM ProceedingsPaper A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given LIKE "%' + name + '%"', con) 
            dfJournal = read_sql('SELECT A.doi, A.publication_year, A.title, A.publication_venue FROM JournalArticle A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given LIKE "%' + name + '%"', con)
            dfBook = read_sql('SELECT A.doi, A.publication_year, A.title, A.publication_venue FROM BookChapter A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given LIKE "%' + name + '%"', con)

        return concat([dfBook, dfJournal, dfProc])  
            
    
    def getDistinctPublisherOfPublications(self, listOfDoi):
        rp0= RelationalProcessor()
        rp0.setDbPath(dbPath)
        tempDF = pd.DataFrame()
        outputDF = pd.DataFrame()
        with connect(rp0.getDbPath()) as con:
            for doi in listOfDoi:
                JournalAsrticleDF = read_sql("SELECT A.id, A.name FROM Organization AS A JOIN Venueid AS B ON A.OrganizationId == B.OrganizationId JOIN JournalArticle AS C ON B.VenueId == C.publication_venue WHERE C.doi = '" + doi + "'", con)
                ProceedingspaperDF = read_sql("SELECT A.id, A.name FROM Organization AS A JOIN Venueid AS B ON A.OrganizationId == B.OrganizationId JOIN ProceedingsPaper AS C ON B.VenueId == C.publication_venue WHERE C.doi = '" + doi + "'", con) 
                BookChapterDF = read_sql("SELECT A.id, A.name FROM Organization AS A JOIN Venueid AS B ON A.OrganizationId == B.OrganizationId JOIN BookChapter AS C ON B.VenueId == C.publication_venue WHERE C.doi = '" + doi + "'", con)  
                tempDF = concat([JournalAsrticleDF, ProceedingspaperDF, BookChapterDF])
                outputDF = concat([outputDF, tempDF])
            
        return outputDF

"""

SQL = "SELECT A.* FROM {} A JOIN (SELECT * FROM Person C JOIN Authors B ON B.orc_id == C.orcid) D ON A.doi == D.doi WHERE D.given = '%{}%'"
            #D.given LIKE "%' + {} + '%
            return concat([
                read_sql(SQL.format(publications[0], name), con),
                read_sql(SQL.format(publications[1], name), con),
                read_sql(SQL.format(publications[2], name), con)
            ])
"""
  
#testList = ["doi:10.1007/s11192-019-03217-6", "doi:10.1162/qss_a_00023"]


rqp = RelationalQueryProcessor()
gqp = GenericQueryProcessor()
#listaQP = rqp.getPublicationsPublishedInYear(2020)

# for object in listaQP:
    
#     print(type(Publication.__str__(object)))
#     break

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
#print(gqp.getPublicationsPublishedInYear(2020))
#print(rqp.getDbPath())
#RelationalQueryProcessor.setDbPath(dbPath)
#print(RelationalQueryProcessor.getDbPath())
#print(gqp.getPublicationsByAuthorId("0000-0001-8686-0017"))

#print(gqp.getPublicationAuthors("doi:10.1162/qss_a_00109"))
#print(rqp.getVenuesByPublisherId("crossref:281"))
#print(rqp.getJournalArticlesInJournal("issn:2641-3337"))
#print(rqp.getVenuesByPublisherId("crossref:281"))
#print(gqp.getPublicationsByAuthorName("Pe"))
#print(rqp.getDistinctPublisherOfPublications([ "doi:10.1162/qss_a_00109", "doi:10.1007/s11192-021-04097-5", "doi:10.3390/su13116225"]))
#print(rqp.getDistinctPublisherOfPublications(testList))

#print(rqp.getProceedingsByEvent("web"))
#print(rqp.getMostCitedPublication())



#print(rqp.getMostCitedVenue())
#print(gqp.getJournalArticlesInIssue("9", "17", "issn:2164-5515"))

# ListaJournalArticleOBJ = gqp.getJournalArticlesInVolume(21,"issn:1616-5187")
# for object in ListaJournalArticleOBJ:
    
#     print(JournalArticle.__str__(object))
#     break

#print(gqp.getJournalArticlesInJournal("issn:1616-5187"))

# ListaJournalArticleOBJ = gqp.getJournalArticlesInJournal("issn:1616-5187")
# for object in ListaJournalArticleOBJ:
    
#     print(JournalArticle.__str__(object))
#     break

# ListaJournalArticleOBJ1 = gqp.getJournalArticlesInIssue(21, 20, "issn:1616-5187")
# for object in ListaJournalArticleOBJ1:
    
#     print(JournalArticle.__str__(object))
#     break

#print(gqp.getJournalArticlesInIssue(21, 20, "issn:1616-5187"))
    


#JADataframe = gqp.getJournalArticlesInVolume(21,"issn:1616-5187")

#print(rqp.getJournalArticlesInIssue(2, 21,"issn:1616-5187"))
#print(JADataframe)



# publicationObj = Publication("doi:10.1162/qss_a_00023	", 2020, "Opencitations, An Infrastructure Organization For Open Scholarship", "Quantitative Science Studies")
# print(type(Publication.__str__(publicationObj)))


#print(gqp.getPublicationInVenue("issn:2641-3337"))


#print(type(dfMCP["cited"]))

#listOfDOI = ["doi:10.1007/s11192-019-03217-6", "doi:10.1007/s11192-021-04097-5", "doi:10.1007/978-3-030-75722-9_7"]
#dataframe = gqp.getDistinctPublisherOfPublications(listOfDOI)
#print(dataframe)
#for object in dataframe:
    #print(Organization.__str__(object))



