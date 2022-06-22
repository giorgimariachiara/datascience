import sqlite3
from sqlite3 import * 
from pandas import DataFrame, concat, read_sql


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

dbPath = "/home/ljutach/Desktop/DHDK_magistrale/courses/DataScience/FinalProject/GitRep/datascience/publications.db"


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
       with connect(rp0.getDbPath()) as con:   # da correggere, usare il metodo appropriato
            con.commit()
           
            JournalArticleDF = read_sql("SELECT * FROM JournalArticle WHERE publication_year = " + str(py), con)
            BookChapterDF = read_sql("SELECT * FROM BookChapter WHERE publication_year = " + str(py), con)
            ProceedingsPaperDF = read_sql("SELECT * FROM ProceedingsPaper WHERE publication_year = " + str(py), con)
            #BookChapterDF = read_sql("SELECT * FROM BookChapter WHERE publication_year = " + str(py) + " LIMIT 2 ", con)
       return(concat([JournalArticleDF, BookChapterDF, ProceedingsPaperDF]))    
   
         
        
#def pathSetter():
  



rqp = RelationalQueryProcessor()
gqp = GenericQueryProcessor()
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
print(gqp.getPublicationsPublishedInYear(2017))

