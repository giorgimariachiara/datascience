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
                    ?s schema:name ?title . \
                    ?s schema:identifier ?doi.\
                    ?s schema:isPartOf ?publicationvenue .\
                    ?s ?p ?o .}')  #controlla output serve drop duplicates 
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
       
        return results 
    
    """
    def getPublicationAuthors(self, publication):
        query = ('prefix schema:<https://schema.org/>  \
                 prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                 SELECT ?name ?surname ?orcid WHERE {?s rdf:type schema:Person . \
                 ?s schema:givenName ?name . \
                 ?s schema:familyName ?surname . \
                 ?s schema:identifier ?orcid . \
                 ?doi schema:author ?orcid . \   
                 ?doi schema:identifier "' + publication + '".}')
    
    def getPublicationsByAuthorId(self, orcid):
        query = ('prefix schema:<https://schema.org/>  \
                  prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\
                    SELECT ?doi ?title ?publicationyear ?publicationvenue WHERE {?s rdf:type schema:CreativeWork.\
                    ?s schema:name ?title . \
                    ?s schema:datePublished ?publicationyear . \
                    ?s schema:isPartOf ?publicationvenue . \
                    ?s schema:identifier ?doi . \                                          
                    ?doi schema:author "' + orcid + '" .\
                    ?s ?p ?o.}')
        endpoint = self.getEndpointUrl()
        results = get(endpoint, query, post = True)
        
        return results 
        
    def getJournalArticlesInJournal(self, issn):
        query = ('prefix schema:<https://schema.org/>  
                prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                prefix bibo:<https://bibliontology.com/>
                 SELECT ?id ?title ?pubyear ?pubvenue ?issue ?volume WHERE {?s rdf:type schema:ScholarlyArticle;
                                                                               	schema:identifier ?id;
                                                                             	schema:name ?title;
                                                                             	schema:datePublished ?pubyear;
                                                                             	schema:isPartOf ?pubvenue;
                                                                                schema:issueNumber "1";
                                                                                schema:volumeNumber "1".
                                                                             	
                                                                             }')

"""


    
    
    
  

