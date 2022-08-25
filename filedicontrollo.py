from impl import TriplestoreDataProcessor
import os
from impl import GenericQueryProcessor, RelationalDataProcessor, RelationalQueryProcessor, TriplestoreProcessor, TriplestoreQueryprocessor
import extraclassesandfunctions
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from extraclassesandfunctions import AddToSparqlStore,  CleanSparqlStore
from extraclassesandfunctions import CleanRelationaldatabase


jsn0 = "./graph_db/graph_other_data.json"
csv0 = "./graph_db/graph_publications.csv"


jsn = "./relational_db/relational_other_data.json"
csv = "./relational_db/relational_publication.csv"
dbpath0 = "publicationgraph.db"
"""
#cleanRelational = CleanRelationaldatabase(dbpath0)
obj = RelationalDataProcessor() 
obj.setDbPath(dbpath0) #primo setting del path al db per caricamento dati
obj.uploadData(jsn0)
obj.uploadData(csv0)
#qp = RelationalQueryProcessor()
#rqp.setDbPath(dbpath0)
#gqp = GenericQueryProcessor() 
#gqp.addQueryProcessor(rqp)
#print(rqp.getPublicationInVenue("issn:0138-9130"))
#print(rqp.getPublicationsPublishedInYear(2020))
#cosa = gqp.getMostCitedPublication()
#print(gqp.Getstringofpythonobject())
"""




#rint(rqp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))




"""
dbpath0 = "publication.db"
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath0)
"""

endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
tqp = TriplestoreQueryprocessor()

tqp.setEndpointUrl(endpointUrl)
#t = TriplestoreDataProcessor()
#clean = CleanSparqlStore(endpointUrl)


#gqp = GenericQueryProcessor() 
#gqp.addQueryProcessor(rqp)
#gqp.addQueryProcessor(tqp)

#print(tqp.getPublicationInVenue("issn:0138-9130"))
#print(rqp.getDistinctPublisherOfPublications([ "doi:10.1016/j.websem.2021.100655", "doi:10.1093/nar/gkz997", "doi:10.3390/publications7030050", "doi:10.1007/978-3-030-33220-4_25"]))
#print(rqp.getProceedingsByEvent("we"))
#gqp = GenericQueryProcessor()
#gqp.addQueryProcessor(rqp)
#gqp.addQueryProcessor(rqp)
#print(tqp.getPublicationsPublishedInYear(2019))
#print(tqp.getProceedingsByEvent("nae"))
#print(tqp.getMostCitedVenue())
#print(tqp.getVenuesByPublisherId("crossref:78"))
#print(tqp.getMostCitedPublication())
#print(tqp.getPublicationsByAuthorName("Pe"))
#print(tqp.getPublicationsByAuthorId("0000-0003-0530-4305"))
#print(tqp.getJournalArticlesInJournal("issn:0138-9130"))
#print(tqp.getJournalArticlesInVolume("17", "issn:2164-5515"))
#print(tqp.getJournalArticlesInIssue("9", "17", "issn:2164-5515"))
print(tqp.getDistinctPublisherOfPublications(["doi:10.3390/publications7030050", "doi:10.1016/j.websem.2021.100655", "doi:10.1016/j.websem.2014.06.002", "doi:10.3390/publications7030050" ]))
#print(rqp.getPublicationAuthors("doi:10.3390/ijfs9030035"))

"""

endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
#cleansparql = CleanSparqlStore(endpointUrl)
csv = "./relational_db/relational_publication.csv"

jsn1 = "./graph_db/graph_other_data.json"
csv1 = "./graph_db/graph_publications.csv"



res = TriplestoreDataProcessor()
res.setEndpointUrl(endpointUrl)
#res.uploadData(csv1)
res.uploadData(jsn1)

# res = TriplestoreDataProcessor()
# res.setEndpointUrl(endpointUrl)
# res.uploadData(jsn1)

#print(res.my_graph.serialize())
"""

"""
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
obj = TriplestoreQueryprocessor()
obj.setEndpointUrl(endpointUrl)
#print(obj.getPublicationsPublishedInYear("2020")) #da gestire le virgolette
#print(obj.getPublicationsByAuthorId("0000-0002-3938-2064"))
print(obj.getDistinctPublisherOfPublications(["doi:10.1016/j.websem.2021.100655", "doi:10.3390/ijfs9030035", "doi:10.1177/01655515211022185"]))

"""
"""

rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData(jsn0)
rel_dp.uploadData(csv)

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

"""
"""
# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("testData/graph_publications.csv")
grp_dp.uploadData("testData/graph_other_data.json")
"""
"""
# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)

# QUERIES AND METHODS
q1 = generic.getPublicationsPublishedInYear(2020)
#print("getPublicationsPublishedInYear Query\n",q1)

print("Methods for the objects of class Publication:\n")
for item in q1:
    print("ITEM")

    print("Method getPublicationYear()\n",item.getPublicationYear())
    print("Method getTitle()\n",item.getTitle())
    print("Method getPublicationVenue()\n",item.getPublicationVenue())
"""