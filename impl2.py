import sqlite3
from sqlite3 import * 

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
        self.queryProcessor = []

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
        xyz = RelationalQueryProcessor.getPublicationsPublishedInYear(publicationYear)
        self.addQueryProcessor(xyz)
        return self.queryProcessor

    def getPublicationsByAuthorId(self, orc_id):
        result = RelationalQueryProcessor.getPublicationByAuthorId(orc_id)

def create_connection(db_file):

    # forse si usa il "with()" 

    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ""

    def setDbPath(self, path: str):
        self.dbPath = path
        return True

    def getDbPath(self):
        return self.dbPath


=======
class QueryProcessor(object):
    def __init__(self):
        pass

class RelationalQueryProcessor(RelationalProcessor):                 #, QueryProcessor):

    def getPublicationsPublishedInYear(publicationYear):
        print(publicationYear)
        print("dahan")
        conn = create_connection(RelationalProcessor.getDbPath())
        cur = conn.cursor()
        cur.execute("SELECT * FROM JournalArticle WHERE publication_year = " + publicationYear + ";")
        return cur.fetchall()











gqp = GenericQueryProcessor()
gqp.getPublicationsPublishedInYear(2020)
print(gqp.queryProcessor)

# gqp.addQueryProcessor("wefwef")
# gqp.addQueryProcessor("qwewqef23r")
# gqp.addQueryProcessor(["dw", 3])


# print(gqp.getQueryProcessorElement(2))
# gqp.cleanQueryProcessors()
# print(gqp.getQueryProcessorElement(0))

