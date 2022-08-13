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
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                    SELECT DISTINCT ?doi ?title ?publicationyear ?publicationvenue WHERE {?s rdf:type schema:CreativeWork.\
                    ?s schema:datePublished "' + publicationYear + '". \
                    ?s schema:datePublished ?publicationyear .\
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi.\
                    ?s schema:isPartOf ?publicationvenue .\
                    }')  
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getPublicationsByAuthorId(self, orcid):
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT ?doiLiteral ?title ?publicationyear ?publicationvenue WHERE {?author schema:identifier "' + orcid +'" . \
                  ?author schema:author ?doi . \
                  ?doi schema:identifier ?doiLiteral . \
                  ?doi schema:name ?title . \
                  ?doi schema:datePublished ?publicationyear . \
                  ?doi schema:isPartOf ?publicationvenue . \
                    }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getMostCitedPublication(self):
        query = ('prefix schema:<https://schema.org/> \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT ?doi ?title ?publicationyear ?publicationvenue WHERE { \
                  ?cited rdf:type schema:CreativeWork . \
                  ?cited schema:identifier ?doi . \
                  ?cited schema:name ?title . \
                  ?cited schema:datePublished ?publicationyear . \
                  ?cited schema:isPartOf ?publicationvenue . \
                    {SELECT ?cited WHERE {FILTER(?N = (MAX(?N))) \
                  {SELECT ?cited (COUNT(*) AS ?N) \
                  WHERE { ?citing schema:citation ?cited . \
                    } GROUP BY ?cited }}}} \
                    ')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 
    
    def getVenuesByPublisherId(self, publisher):
        query = ('prefix schema:<https://schema.org/> \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                 SELECT DISTINCT ?venueid ?publication_venue ?crossref WHERE {?publisher schema:identifier "' + publisher + '" . \
                 ?publisher schema:identifier ?crossref . \
                 ?doi schema:publisher ?publisher . \
                 ?doi rdf:type schema:CreativeWork . \
                 ?doi schema:isPartOf ?publication_venue .  \
                 ?venueid schema:name ?publication_venue .  \
                  }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getPublicationInVenue(self, issn_isbn):
        query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                  prefix schema:<https://schema.org/> \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT ?doiLiteral ?title ?publicationyear ?publicationvenue WHERE {?doi dcterms:identifier "' + issn_isbn +'" . \
                    ?doi schema:identifier ?doiLiteral . \
                    ?doi schema:name ?title . \
                    ?doi schema:datePublished ?publicationyear  .\
                    ?doi schema:isPartOf ?publicationvenue . \
                    }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 
    
    def getProceedingsByEvent(self, eventPartialName): 
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
    
    def getJournalArticlesInVolume(self, volume, issn_isbn): 
        query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                  prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>  \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                 ?s schema:name ?title . \
                 ?s schema:identifier ?doi .  \
                 ?s schema:volumeNumber "'+ volume +'" .  \
                 ?s schema:isPartOf ?publicationVenue . \
                 ?s schema:datePublished ?publicationYear . \
                 ?s dcterms:identifier "'  + issn_isbn + '". \
                 OPTIONAL {  \
                 ?s schema:issueNumber ?issue } \
                         }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getJournalArticlesInIssue(self, issue, volume, issn_isbn):
        query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                  prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                  ?s schema:name ?title . \
                  ?s schema:identifier ?doi . \
                  ?s dcterms:identifier "'  + issn_isbn + '" . \
                 ?s schema:issueNumber "' + issue + '". \
                 ?s schema:volumeNumber "'+ volume +'" . \
                 ?s schema:isPartOf ?publicationVenue .\
                 ?s schema:datePublished ?publicationYear .  \
                }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getJournalArticlesInJournal(self, issn_isbn): 
        query = ('prefix dcterms:<http://purl.org/dc/terms/> \
                 prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                  ?s schema:name ?title . \
                  ?s schema:identifier ?doi . \
                  ?s dcterms:identifier "'  + issn_isbn + '". \
                 ?s schema:isPartOf ?publicationVenue .\
                 ?s schema:datePublished ?publicationYear .  \
				 OPTIONAL { \
                 ?s schema:issueNumber ?issue }.\
          	    OPTIONAL {?s schema:volumeNumber ?volume  \
                }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getPublicationAuthors(self, publication):
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
    
    def getPublicationsByAuthorName(self, name):
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT ?doiLiteral ?title ?publicationyear ?publicationvenue WHERE {?author schema:author ?doi . \
                  ?author schema:givenName ?name. \
                  ?doi schema:identifier ?doiLiteral . \
                  ?doi schema:name ?title .  \
                  ?doi schema:datePublished ?publicationyear .  \
                  ?doi schema:isPartOf ?publicationvenue . \
                  filter contains(?name,"' + name +'") ')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
                
        return results

    def getDistinctPublisherOfPublications(self, lista):
        publisher = pd.DataFrame()
        for el in lista:
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
    

"""
getmostcitedvenue da controllare 
prefix schema:<https://schema.org/>  
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?venueid ?publicationvenue ?publisher 
WHERE {
  ?cited rdf:type schema:CreativeWork .
  ?cited schema:identifier ?doi .
  ?cited schema:publisher ?publisher .
  ?cited schema:isPartOf ?publicationvenue .
  ?venueid schema:name ?publicationvenue . 
{SELECT ?cited WHERE {FILTER(?N = (MAX(?N)))
{SELECT ?cited (COUNT(*) AS ?N) 
WHERE { ?citing schema:citation ?cited .
  } GROUP BY ?cited }}}}


"""