from impl import GenericQueryProcessor, RelationalDataProcessor, RelationalQueryProcessor, TriplestoreQueryProcessor, TriplestoreDataProcessor
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from extraclassesandfunctions import AddToSparqlStore,  CleanSparqlStore
from extraclassesandfunctions import CleanRelationaldatabase

jsngraph = "./graph_db/graph_other_data.json"
csvgraph = "./graph_db/graph_publications.csv"

jsnrel = "./relational_db/relational_other_data.json"
csvrel  = "./relational_db/relational_publication.csv"
dbpath = "publications.db"


#RELATIONAL DATABASE STEPS 
cleanRelational = CleanRelationaldatabase(dbpath)
obj = RelationalDataProcessor() 
obj.setDbPath(dbpath) #primo setting del path al db per caricamento dati
obj.uploadData(jsnrel)
obj.uploadData(csvrel)
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath)


#GRAPH DATABASE STEPS 
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
cleansparql = CleanSparqlStore(endpointUrl)
obj = TriplestoreDataProcessor()
obj.setEndpointUrl(endpointUrl)
obj.uploadData(jsngraph)
obj.uploadData(csvgraph)
tqp = TriplestoreQueryProcessor()
tqp.setEndpointUrl(endpointUrl)



#GENERIC DATABASE STEP 

gqp = GenericQueryProcessor() 
gqp.addQueryProcessor(rqp)
gqp.addQueryProcessor(tqp)



#print(tqp.getPublicationInVenue(2020))
#print(tqp.getDistinctPublisherOfPublications("string"))
#print(rqp.getProceedingsByEvent("we"))
#gqp = GenericQueryProcessor()
#gqp.addQueryProcessor(rqp)
#gqp.addQueryProcessor(rqp)
#print(rqp.getPublicationsPublishedInYear("s"))
#print(gqp.getProceedingsByEvent("nae"))
#print(gqp.getMostCitedVenue())
#print(gqp.getVenuesByPublisherId("crossref:78"))
#print(gqp.getMostCitedPublication())
#print(gqp.getPublicationsByAuthorName("Pe"))
#print(gqp.getPublicationsByAuthorId("0000-0003-0530-4305"))
#print(gqp.getJournalArticlesInJournal("issn:0138-9130"))
#print(gqp.getJournalArticlesInVolume("17", "issn:2164-5515"))
#print(gqp.getJournalArticlesInIssue("9", "17", "issn:2164-5515"))
#print(qp.getDistinctPublisherOfPublications(["doi:10.1016/j.websem.201.100655", "doi:10.1016/j.websem.2014.06.002", "doi:10.3390/publications7030050"]))
#print(gqp.getPublicationAuthors("doi:10.3390/ijfs9030035"))