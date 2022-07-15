from implRel import *
import rdflib


endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'

class TripleQueryProcessor(TriplestoreProcessor, QueryProcessor):
    def getPublicationsPublishedInYear(self, py):
        tp0 = TriplestoreProcessor() 
        tp0.setEndpointUrl(endpointUrl)
        tp0.getEndpointUrl()
        query = """ 
        SELECT *
        WHERE {
            ?s rdf:type schema:ScholarlyArticle.
            ?s schema:datePublished "2021".
            ?s ?p ?o
        }
        """
        ciao = tp0.query(query)
        print(ciao)
        
        
        