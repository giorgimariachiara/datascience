from logging import raiseExceptions
from rdflib import Graph, URIRef, Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from SPARQLWrapper import CSV, JSON, TSV, SPARQLWrapper
import pandas as pd
from pandas import DataFrame, concat
from graph import TriplestoreDataProcessor
from impl import QueryProcessor, TriplestoreProcessor
from sparql_dataframe import get

class TriplestoreQueryprocessor(TriplestoreProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self, publicationYear):
        if type(publicationYear) == int:
            query = ('prefix schema:<https://schema.org/>  \
                    prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                    SELECT DISTINCT ?doi ?title ?publicationyear ?venue WHERE {?s rdf:type schema:CreativeWork.\
                    ?s schema:datePublished "' + str(publicationYear) + '". \
                    ?s schema:datePublished ?publicationyear .\
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi. \
                    ?s schema:isPartOf ?publicationvenue . \
                    ?publicationvenue schema:name ?venue .  \
                    }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else: 
            raiseExceptions("The input parameter publicationYear is not an integer!")

    def getPublicationsByAuthorId(self, orcid):
        if type(orcid) == str:
            query = ('prefix schema:<https://schema.org/>  \
                     SELECT ?doiLiteral ?title ?publicationyear ?venue WHERE {?author schema:identifier "' + orcid + '" . \
                     ?author schema:author ?doi . \
                     ?doi schema:identifier ?doiLiteral . \
                     ?doi schema:name ?title .  \
                     ?doi schema:datePublished ?publicationyear .  \
                     ?doi schema:isPartOf ?publicationvenue . \
                     ?publicationvenue schema:name ?venue . \
                        }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else: 
            raiseExceptions("The input parameter orcid is not a string!")

    def getMostCitedPublication(self):
        query = ('prefix schema: <https://schema.org/> \
                 SELECT ?doi ?publicationyear ?title ?venuename WHERE {?citing schema:identifier ?doi . \
                ?citing schema:name ?title . \
                ?citing schema:datePublished ?publicationyear . \
                ?citing schema:isPartOf ?venueid . \
                ?venueid schema:name ?venuename .{ \
                SELECT ?citing WHERE { \
                { SELECT ?citing (COUNT(?x) as ?count) WHERE {   ?citing schema:citation ?x . } GROUP BY ?citing } \
                { SELECT (MAX(?cited) AS ?count) WHERE { \
    	        { SELECT ?citing (COUNT(?x) as ?cited) WHERE {  ?citing schema:citation ?x . }  GROUP BY ?citing }  \
                    }}}}}')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getMostCitedVenue(self):
         query= (' prefix schema: <https://schema.org/> \
                    SELECT ?id ?venuename ?publisher  WHERE {?citing schema:identifier ?doi . \
                    ?citing schema:isPartOf ?venueid . \
                    ?venueid schema:identifier ?id . \
                    ?venueid schema:name ?venuename . \
                    ?venueid schema:publisher ?publisher.  \
                    {SELECT ?citing WHERE { \
                    { SELECT ?citing (COUNT(?x) as ?count) WHERE {   ?citing schema:citation ?x . } GROUP BY ?citing } \
                    { SELECT (MAX(?cited) AS ?count) WHERE { \
    	            { SELECT ?citing (COUNT(?x) as ?cited) WHERE {  ?citing schema:citation ?x . }  GROUP BY ?citing }  \
                        }}}}}')
         endpoint = self.getEndpointUrl()
         results = get(endpoint, query, post = True)
         
         return results
    
    def getVenuesByPublisherId(self, publisher):
        if type(publisher) == str:
            query = ('prefix schema:<https://schema.org/> \
                    SELECT DISTINCT ?venueid ?publicationvenuename ?crossref WHERE { ?doi schema:isPartOf ?venue . \
                      ?venue schema:identifier ?venueid . \
                      ?venue schema:publisher "' + publisher + '" . \
                      ?venue schema:publisher ?crossref .  \
                      ?venue schema:name ?publicationvenuename . \
                       }')                                                       
                      
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else: 
            raiseExceptions("The input parameter publisher is not a string!")

    def getPublicationInVenue(self, issn_isbn):
        if type(issn_isbn) == str:
            query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                    prefix schema:<https://schema.org/> \
                    SELECT ?doiLiteral ?title ?publicationyear ?venuename WHERE {?doi dcterms:identifier "' + issn_isbn + '" . \
                        ?doi schema:identifier ?doiLiteral . \
                        ?doi schema:name ?title . \
                        ?doi schema:datePublished ?publicationyear  . \
                        ?doi schema:isPartOf ?publicationvenue . \
                        ?publicationvenue schema:name ?venuename . \
                                       }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results
        else: 
            raiseExceptions("The input parameter issn_isbn is not a string!")
    
    def getProceedingsByEvent(self, eventPartialName):
        if type(eventPartialName) == str: 
            query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                    prefix schema:<https://schema.org/>  \
                    SELECT ?issn_isbn ?publication_venue ?publisher ?event WHERE {?s schema:event ?event. \
                    ?s schema:name ?publication_venue . \
                    ?doi schema:isPartOf ?publication_venue . \
                    ?doi schema:publisher ?publisher . \
                    ?doi dcterms:identifier ?issn_isbn . \
                    filter contains(?event,"' + eventPartialName +'") \
                    }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results
        else: 
            raiseExceptions("The input parameter eventPartialName is not a string!")
    
    def getJournalArticlesInVolume(self, volume, issn_isbn): 
        if type(volume) == str and type(issn_isbn) == str: 
            query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                    prefix schema:<https://schema.org/>  \
                    prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>  \
                    SELECT DISTINCT ?doi ?publicationYear ?venue ?title ?issue ?volume  \
                    WHERE { ?s rdf:type schema:ScholarlyArticle  .\
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi .  \
                    ?s dcterms:identifier "' + issn_isbn + '". \
                    ?s schema:volumeNumber "' + volume + '" . \
                    ?s schema:volumeNumber ?volume .\
                    ?s schema:isPartOf ?publicationVenue .\
                    ?publicationVenue schema:name ?venue .\
                    ?s schema:datePublished ?publicationYear . \
                    OPTIONAL {  \
                    ?s schema:issueNumber ?issue } \
                           }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else:
            raiseExceptions("All or one of the input parameters volume and issn_isbn is not a string!") 

    def getJournalArticlesInIssue(self, issue, volume, issn_isbn):
        if type(issue) == str and type(volume) == str and type(issn_isbn) == str:
            query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                    prefix schema:<https://schema.org/>  \
                    prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                    SELECT DISTINCT ?doi ?publicationYear ?venue ?title ?issue ?volume  \
                    WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi . \
                    ?s dcterms:identifier "'  + issn_isbn + '" . \
                    ?s schema:issueNumber "' + issue + '". \
                    ?s schema:volumeNumber "'+ volume +'" . \
                    ?s schema:isPartOf ?publicationVenue .\
                    ?publicationVenue schema:name ?venue .\
                    ?s schema:datePublished ?publicationYear .  \
                    }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else: 
            raiseExceptions("All or one of the input parameters issue, volume and issn_isbn is not a string!") 


    def getJournalArticlesInJournal(self, issn): 
        if type(issn) == str:
            query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                    prefix schema:<https://schema.org/>  \
                    prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                    SELECT DISTINCT ?doi ?publicationYear ?venue ?title ?issue ?volume  \
                    WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi . \
                    ?s dcterms:identifier "'  + issn + '". \
                    ?s schema:isPartOf ?publicationVenue .\
                    ?publicationVenue schema:name ?venue .\
                    ?s schema:datePublished ?publicationYear .  \
                    OPTIONAL { \
                    ?s schema:issueNumber ?issue }.\
                    OPTIONAL {?s schema:volumeNumber ?volume  \
                    }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
            
            return results 
        else: 
            raiseExceptions("The input parameter issn is not a string!")

    def getPublicationAuthors(self, publication):
        if type(publication) == str:
            query = ('prefix schema:<https://schema.org/>  \
                    prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                    SELECT ?name ?surname ?orcid WHERE {?s rdf:type schema:Person . \
                    ?s schema:author ?doi . \
                    ?doi schema:identifier "' + publication + '" . \
                    ?s schema:familyName ?surname . \
                    ?s schema:givenName ?name . \
                    ?s schema:identifier ?orcid . \
                    }')                                                             
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
                    
            return results
        else: 
            raiseExceptions("The input parameter publication is not a string!")
    
    def getPublicationsByAuthorName(self, name):
        if type(name) == str:
            query = ('prefix schema:<https://schema.org/>  \
                    SELECT ?doiLiteral ?title ?publicationyear ?venue WHERE {?author schema:author ?doi . \
                    ?author schema:givenName ?name. \
                    ?doi schema:identifier ?doiLiteral . \
                    ?doi schema:name ?title .  \
                    ?doi schema:datePublished ?publicationyear .  \
                    ?doi schema:isPartOf ?publicationvenue . \
                    ?publicationVenue schema:name ?venue .\
                    filter contains(?name,"' + name +'") }')
            endpoint = self.getEndpointUrl()
            results = get(endpoint, query, post = True)
                    
            return results
        else: 
            raiseExceptions("The input parameter name is not a string!")

    def getDistinctPublisherOfPublications(self, listOfDoi):
        for el in listOfDoi:
            if type(el) == str and type(listOfDoi) == list:
                publisher = pd.DataFrame()
                for el in listOfDoi:
                    query = ('prefix schema:<https://schema.org/>  \
                            prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                            SELECT DISTINCT ?name ?id WHERE {?doi rdf:type schema:CreativeWork . \
                            ?doi schema:identifier "'+ el +'". \
                            ?doi schema:publisher ?publisher . \
                            ?publisher schema:name ?name . \
                            ?publisher schema:identifier ?id . \
                            }')
                    endpoint = self.getEndpointUrl()
                    results = get(endpoint, query, post= True)
                    publisher = concat([publisher, results])
                return publisher
            else: 
                raiseExceptions("The input parameter listOfDoi is not a list or one of its elements is not a string!")
    