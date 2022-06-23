#<<<<<<< HEAD
# #defining classes 
# from typing_extensions import Self
# import pandas as pd
# from pandas import DataFrame, Series 
# import os 
#=======
#defining classes 
import pandas as pd
from pandas import DataFrame, Series 
import os 

  
"""
# #----------------------------------------


<<<<<<< HEAD
# class RelationalProcessor(object):
#     def __init__(self, dbPath):
#         self.dbPath = '' 
=======
class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = '' 

       
#     def getDbPath(self):
#         return self.dbPath 

#     def setDbPath(self, Path):
#         self.dbPath = Path 

# #triplestoreprocessor class da fare


# #----------------------------------------

<<<<<<< HEAD
# class QueryProcessor(object):
#     pass
=======
class QueryProcessor(object):
    def __init__(self):
        pass


#----------------------------------------

 
# class RelationalDataProcessor(RelationalProcessor, QueryProcessor):
#     def __init__(self):
#         super().__init__()

#     def uploadData(self,path):

<<<<<<< HEAD
#         if path.split(".")[1] == 'json':
#             #checking if database exists or not
#             if os.path.exists(self.getDbPath()):
#                 pass
#               #if existed do things....
            
#             #else: 
#             #start create dataframes and so on...
# #----------------------------------------
=======
        if path.split(".")[1] == 'csv':
            #checking if database exists or not
            if os.path.exists(self.getDbPath()):
                pd.read_csv(self.dbPath)

            elif path.split(".")[1] == 'json':
                pd.read_json(self.dbPath)

            else: 
                self.setDbPath(path)

#----------------------------------------

# #----------------------------------------

<<<<<<< HEAD
# class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
#     def __init__(self):
#         super().__init__()
=======
class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()
    
#     def getPublicationsPublishedInYear(publicationYear):
#         result = list()
#         for year in Publication.publicationYear:
#             if year == publicationYear:
#                 result.append(Publication.title)

<<<<<<< HEAD
#             return pd.Dataframe({
#                 "Publication": Series(result, dtype="string", name="Publication")
#                 })     
=======
            return result 
    
#     def getPublicationsbyAuthorID():
#         pass 
    
<<<<<<< HEAD




# class GenericQueryProcessor(object):
#     def __init__(self, queryProcessor):
#         self.queryProcessor = []
=======
#----------------------------------------
"""
"""
class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = list()
>>>>>>> 2478b39acd570bfc592d7bff6c8385350f746c4a
    
#     def cleanQueryProcessors(self, queryProcessor):
#         queryProcessor.clear() 

<<<<<<< HEAD
#     def addQueryProcessor(self, QueryProcessor, queryProcessor):
#         result = queryProcessor.append(QueryProcessor)
#         return result 
=======
    def addQueryProcessor(self, queryProcessor):
        pass
>>>>>>> 2478b39acd570bfc592d7bff6c8385350f746c4a

#  #   def getPublicationsPublishedInYear(year):
#    #     result= []
#     #   for date in publicationYear: 
#        #     if date == year:
#          #       result = Publication.getPublicationYear()

<<<<<<< HEAD
# #----------------------------------------

# class IdentifiableEntity(object):
#     def __init__(self, id):
#         self.id = set()
#         for identifiers in id:
#             self.id.add(identifiers)
=======
#----------------------------------------

class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = set()
        for identifiers in id:
            self.id.add(identifiers)

#     def getIds(self):
#         result = []
#         for identifier in self.id:
#             result.append(identifier)
#         result.sort()
#         return result

# #----------------------------------------







# class Publication(IdentifiableEntity):
#     def __init__(self, id, publicationYear, title, publicationVenue, cite, author):
#         self.publicationYear = publicationYear
#         self.title = title
#         self.publicationVenue = publicationVenue
#         self.cite = cite
#         self.author = author 
#         super().__init__(id)

#     def getPublicationYear(self):
#         if self.publicationYear:
#             return self.publicationYear
#         else:
#             return None
    
#     def getTitle(self):
#         return self.title
    
#     def getCitedPublications(self):
#         result= []
#         for citations in self.cite:
#             result.append(citations)
#         return result 

#     def getPublicationVenue(self):
#         return self.publicationVenue

#     def getAuthors(self):
#         result = set()
#         for p in self.author:
#             result.add(p)
#         return result

# #----------------------------------------

# class Venue(IdentifiableEntity):
#     def __init__(self, id, title, publisher):
#         self.title = title
#         self.publisher = publisher 
#         super().__init__(id)
        
#     def getTitle(self):
#         return self.title

#     def getPublisher(self):
#         return self.publisher

# #----------------------------------------

# class Organization(IdentifiableEntity):
#     def __init__(self, id, name):
#         self.name = name
#         super().__init__(id)

#     def getName(self):
#         return self.name

# #----------------------------------------        

# class Person(IdentifiableEntity):
#     def __init__(self, id, givenName, familyName):
#         self.givenName = givenName 
#         self.familyName = familyName
#         super().__init__(id)

#     def getGivenName(self):
#         return self.givenName
    
#     def getFamilyName(self):
#         return self.familyName


# class JournalArticle(Publication):
#     def __init__(self, id, publicationYear, title, publicationVenue, cite, author, issue, volume):
#         self.issue = issue
#         self.volume = volume
#         super().__init__(id, publicationYear, title, publicationVenue, cite, author)

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

    

# class BookChapter(Publication):
#     def __init__(self, id, publicationYear, title, publicationVenue, cite, author, chapterNumber):
#         self.chapterNumber = chapterNumber
          
#         super().__init__(id, publicationYear, title, publicationVenue, cite, author)
    
#     def getChapterNumber(self):
#         return self.chapterNumber

# class ProceedingsPaper(Publication):
#     pass

# class Journal(Venue):
#     pass


# class Book(Venue):
#     pass

# class Proceedings(Venue):
#     def __init__(self, id, title, publisher, event):
#         self.event = event
#         super().__init__(id, title, publisher)
   
#     def getEvent(self): 
#         return self.event


"""


