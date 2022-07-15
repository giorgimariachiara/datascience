from implRel import *
import rdflib


endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'

class TripleQueryProcessor(TriplestoreProcessor, QueryProcessor):
    def getPublicationsPublishedInYear(self, py):
        g = rdflib.Graph()
        g.parse("http://127.0.0.1:9999/blazegraph/sparql")
        #tp0 = TriplestoreProcessor() 
        #tp0.setEndpointUrl(endpointUrl)
        #tp0.getEndpointUrl()
        query = """ 
        SELECT *
        WHERE {
            SERVICE <http://127.0.0.1:9999/blazegraph/sparql> {
            ?s rdf:type schema:ScholarlyArticle.
            ?s schema:datePublished "2021".
            ?s ?p ?o }
        }
        """
<<<<<<< Updated upstream
        ciao = tp0.query(query)
=======
        ciao = g.query(query)
>>>>>>> Stashed changes
        print(ciao)
        
        
        