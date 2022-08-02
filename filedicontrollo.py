from graph import TriplestoreDataProcessor
import os
from impl import GenericQueryProcessor, RelationalDataProcessor, RelationalQueryProcessor, TriplestoreProcessor
from provagr import TriplestoreQueryprocessor
import extraclasses
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
 

jsn0 = "./relational_db/relational_other_data.json"
csv = "./relational_db/relational_publication.csv"
dbpath0 = "publication.db"
"""
if os.path.exists(dbpath0):
    os.remove(dbpath0)
"""
"""
obj = RelationalDataProcessor() 
obj.setDbPath(dbpath0) # primo setting del path al db per caricamento dati
obj.uploadData(jsn0)
obj.uploadData(csv)
rqp = RelationalQueryProcessor()
rqp.getDbPath()
print(rqp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))
"""
"""
dbpath0 = "publication.db"
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath0)

#endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
#tqp = TriplestoreQueryprocessor()
#tqp.setEndpointUrl(endpointUrl)

#print(rqp.getDistinctPublisherOfPublications([ "doi:10.1016/j.websem.2021.100655", "doi:10.1093/nar/gkz997", "doi:10.3390/publications7030050", "doi:10.1007/978-3-030-33220-4_25"]))
#print(rqp.getProceedingsByEvent("we"))
gqp = GenericQueryProcessor()
gqp.addQueryProcessor(rqp)
#gqp.addQueryProcessor(rqp)

print(gqp.getProceedingsByEvent("name"))

#print(gqp.getVenuesByPublisherId("crossref:78"))
#print(gqp.getMostCitedPublication())
#print(gqp.getPublicationsByAuthorId("0000-0003-0530-4305"))
#print(gqp.getJournalArticlesInJournal("issn:0138-9130"))
#print(gqp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))
#for el in resultq1:
    #print(el.__str__())
"""

"""

csv = "./relational_db/relational_publication.csv"

jsn1 = "./graph_db/graph_other_data.json"
csv1 = "./graph_db/graph_publications.csv"
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'

store = SPARQLUpdateStore()
store.open((endpointUrl, endpointUrl))
store.remove((None, None, None), context=None)
store.close()

res = TriplestoreDataProcessor()
res.setEndpointUrl(endpointUrl)
res.uploadData(csv1)
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