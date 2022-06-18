#defining classes 
from typing_extensions import Self
import pandas as pd
from pandas import DataFrame
import os 
  

#----------------------------------------


class RelationalProcessor(object):
    def __init__(self, dbPath):
        self.dbPath = '' 
       
    def getDbPath(self):
        return self.dbPath 

    def setDbPath(self, Path):
        self.dbPath = Path 

#triplestoreprocessor class da fare


#----------------------------------------

class QueryProcessor(object):
    pass
 
class RelationalDataProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self,path):

        if path.split(".")[1] == 'json':
            #checking if database exists or not
            if os.path.exists(self.getDbPath()):
                pass
              #if existed do things....
            
            #else: 
            #start create dataframes and so on...
#----------------------------------------

#----------------------------------------

class RelationalQueryProcessor(RelationalProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(publicationYear):
        result = list()
        for year in Publication.publicationYear:
            if year == publicationYear:
                result.apppend(Publication.title)

            return pd.Dataframe(result)




class GenericQueryProcessor(object):
    def __init__(self, queryProcessor):
        self.queryProcessor = []
    
    def cleanQueryProcessors(self, queryProcessor):
        queryProcessor.clear() 

    def addQueryProcessor(self, QueryProcessor, queryProcessor):
        result = queryProcessor.append(QueryProcessor)
        return result 

 #   def getPublicationsPublishedInYear(year):
   #     result= []
    #   for date in publicationYear: 
       #     if date == year:
         #       result = Publication.getPublicationYear()

#----------------------------------------

class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = set()
        for identifiers in id:
            self.id.add(identifiers)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result

#----------------------------------------

class Publication(IdentifiableEntity):
    def __init__(self, id, publicationYear, title, publicationVenue, cite, author):
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.cite = cite
        self.author = author 
        super().__init__(id)

    def getPublicationYear(self):
        if self.publicationYear:
            return self.publicationYear
        else:
            return None
    
    def getTitle(self):
        return self.title
    
    def getCitedPublications(self):
        result= []
        for citations in self.cite:
            result.append(citations)
        return result 

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        result = set()
        for p in self.author:
            result.add(p)
        return result

#----------------------------------------

class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title
        self.publisher = publisher 
        super().__init__(id)
        
    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher

#----------------------------------------

class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name

class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):
        self.givenName = givenName 
        self.familyName = familyName
        super().__init__(id)

    def getGivenName(self):
        return self.givenName
    
    def getFamilyName(self):
        return self.familyName


class JournalArticle(Publication):
    def __init__(self, id, publicationYear, title, publicationVenue, cite, author, issue, volume):
        self.issue = issue
        self.volume = volume
        super().__init__(id, publicationYear, title, publicationVenue, cite, author)

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
    def __init__(self, id, publicationYear, title, publicationVenue, cite, author, chapterNumber):
        self.chapterNumber = chapterNumber
          
        super().__init__(id, publicationYear, title, publicationVenue, cite, author)
    
    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    pass

class Journal(Venue):
    pass


class Book(Venue):
    pass

class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event
        super().__init__(id, title, publisher)
   
    def getEvent(self): 
        return self.event





