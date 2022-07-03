from posixpath import split
import sqlite3
from sqlite3 import * 
from pandas import DataFrame, concat, read_sql
import pandas
from mimetypes import init
from unicodedata import name

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

class Venue(IdentifiableEntity): #issn_isbn serve o no? 
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

class GraphProcessor(object):
    def __init__(self):
        self.endpointUrl = ""

    def setEndpointUrl(self, url):
        self.endpointUrl = url
        return True

    def getEndpointUrl(self):
        return self.endpointUrl

class QueryProcessor(object):
    def __init__(self):
        pass
    

class GraphQueryProcessor(GraphProcessor, QueryProcessor):

     def getPublicationsPublishedInYear(self, py):
        