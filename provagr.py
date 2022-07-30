from rdflib import Graph, URIRef, Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from SPARQLWrapper import CSV, JSON, TSV, SPARQLWrapper

from graph import TriplestoreDataProcessor
from implRel import QueryProcessor, TriplestoreProcessor
from sparql_dataframe import get

#doi e issn hanno lo stesso schema e quindi prende sempre tutti e due secondo me non va bene 
"""
sparql = SPARQLWrapper("http://localhost:9999/blazegraph/sparql") 
sparql.setQuery("SELECT (COUNT(*) AS ?ntpls) WHERE {?s ?p ?o}")
sparql.setReturnFormat(JSON)
results = sparql.query().convert() 
for result in results["results"]["bindings"]:
    print(result["ntpls"]["value"])
"""
"""
if __name__ == "__main__":

    dbo = Namespace("http://dbpedia.org/ontology/")

    # EXAMPLE 3: doing RDFlib triple navigation using SPARQLStore as a Graph()
    print("Triple navigation using SPARQLStore as a Graph():")
    graph = Graph("SPARQLStore", identifier="http://dbpedia.org")
    graph.open("http://dbpedia.org/sparql")
    # we are asking DBPedia for 3 skos:Concept instances
    count = 0
    from rdflib.namespace import RDF, SKOS

    for s in graph.subjects(predicate=RDF.type, object=SKOS.Concept):
        count += 1
        print(f"\t- {s}")
        if count >= 3:
            break
  """  

class TriplestoreQueryprocessor(TriplestoreProcessor, QueryProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self, publicationYear):
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\
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
                 SELECT DISTINCT ?venueid ?publication_venue ?crossref WHERE {?publisher schema:identifier "' + publisher '" . \
                 ?publisher schema:identifier ?crossref . \
                 ?doi schema:publisher ?publisher . \
                 ?doi rdf:type schema:CreativeWork . \
                 ?doi schema:isPartOf ?publication_venue .  \
                 ?venueid schema:name ?publication_venue . \
                  } \
                    ')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getPublicationInVenue(self, issn_isbn):
        query = ('prefix schema:<https://schema.org/> \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT ?doiLiteral ?title ?publicationyear ?publicationvenue WHERE {?doi <http://gbol.life/ontology/bibo/identifier/> "' + issn_isbn +'" . \
                    ?doi schema:identifier ?doiLiteral . \
                    ?doi schema:name ?title . \
                    ?doi schema:datePublished ?publicationyear  .\
                    ?doi schema:isPartOf ?publicationvenue . \
                    }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 
    
    def getProceedingsByEvent(self, eventPartialName):  #qui vanno bene le percentuali per il nome 
        query = ('prefix schema:<https://schema.org/>  \
                  prefix bibo:<https://bibliontology.com/> \
                  SELECT ?issn_isbn ?publication_venue ?publisher ?event WHERE {?s schema:event "%' + eventPartialName + '%" . \
                  ?s schema:name ?publication_venue . \
                  ?doi schema:isPartOf ?publication_venue . \
                  ?doi schema:publisher ?publisher . \
                  ?doi bibo:identifier ?issn_isbn . \
                 }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 
    
    def getJournalArticlesInVolume(self, volume, issn_isbn): #risolvere ontologia ho messo distinct perchè me li irpeteva non so se è un proboema 
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                  ?s schema:name ?title . \
                  ?s schema:identifier ?doi . \
                  ?s <http://gbol.life/ontology/bibo/identifier/> "'  + issn_isbn + '" . \
                 ?s schema:issueNumber ?issue . \
                 ?s schema:volumeNumber "'+ volume +'" . \
                 ?s schema:volumeNumber ?volume . \
                 ?s schema:isPartOf ?publicationVenue .\
                 ?s schema:datePublished ?publicationYear .  \
                }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getJournalArticlesInIssue(self, issue, volume, issn_isbn): #risolvere ontologia 
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                  ?s schema:name ?title . \
                  ?s schema:identifier ?doi . \
                  ?s <http://gbol.life/ontology/bibo/identifier/> "'  + issn_isbn + '" . \
                 ?s schema:issueNumber "' + issue + '". \
                 ?s schema:volumeNumber "'+ volume +'" . \
                 ?s schema:volumeNumber ?volume . \
                 ?s schema:isPartOf ?publicationVenue .\
                 ?s schema:datePublished ?publicationYear .  \
                }')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 

    def getJournalArticlesInJournal(self, issn_isbn): #risolvere ontologia 
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                  SELECT DISTINCT ?doi ?publicationYear ?publicationVenue ?title ?issue ?volume  \
                  WHERE { ?s rdf:type schema:ScholarlyArticle  . \
                  ?s schema:name ?title . \
                  ?s schema:identifier ?doi . \
                  ?s <http://gbol.life/ontology/bibo/identifier/> "'  + issn_isbn + '" . \
                 ?s schema:issueNumber ?issue . \
                 ?s schema:volumeNumber ?volume . \
                 ?s schema:volumeNumber ?volume . \
                 ?s schema:isPartOf ?publicationVenue .\
                 ?s schema:datePublished ?publicationYear .  \
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
    


"""
prefix schema:<https://schema.org/>
prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
SELECT DISTINCT * #?venueid ?publication_venue ?crossref 
WHERE { ?doi schema:isPartOf ?publication_venue . 
?doi rdf:type schema:CreativeWork . 
?doi schema:publisher ?crossref . 
#?publisher rdf:type schema:Organization .
#?publisher schema:identifier "crossref:140" .
#?publisher schema:identifier ?crossref . 
#?doi schema:publisher ?publisher . 
#?doi schema:isPartOf ?publication_venue . 
#?venueid schema:name ?publication_venue . 
} ORDER BY ?publication_venue



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