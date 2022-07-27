from graph import Publication, TriplestoreDataProcessor
import os
from implRel import GenericQueryProcessor, RelationalQueryProcessor, RelationalDataProcessor, TriplestoreProcessor
from provagr import TriplestoreQueryprocessor


"""
jsn0 = "./relational_db/relational_other_data.json"
csv = "./relational_db/relational_publication.csv"
dbpath0 = "publication.db"
if os.path.exists(dbpath0):
    os.remove(dbpath0)

obj = RelationalDataProcessor() 
obj.setDbPath(dbpath0) # primo setting del path al db per caricamento dati
obj.uploadData(jsn0)
obj.uploadData(csv)


"""
"""
dbpath0 = "publication.db"
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath0)
print(rqp.getVenuesByPublisherId("crossref:78"))
"""

#rqp = RelationalQueryProcessor()
#rqp.setDbPath(dbpath0)
#print(rqp.getVenuesByPublisherId("crossref:78"))

#gqp = GenericQueryProcessor()
#qp.addQueryProcessor(rqp)
#print(gqp.getVenuesByPublisherId("crossref:78"))
#print(gqp.getMostCitedPublication())
#print(gqp.getPublicationsByAuthorId("0000-0003-0530-4305"))
#print(gqp.getJournalArticlesInJournal("issn:0138-9130"))
#print(gqp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))
#for el in resultq1:
    #print(el.__str__())



jsn1 = "./graph_db/graph_other_data.json"
csv1 = "./graph_db/graph_publications.csv"
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
res = TriplestoreDataProcessor()
res.setEndpointUrl(endpointUrl)
res.uploadData(csv1)
print(res.my_graph.serialize())


"""


"""


endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
obj = TriplestoreQueryprocessor()
obj.setEndpointUrl(endpointUrl)
print(obj.getPublicationsPublishedInYear("2020")) #da gestire le virgolette
"""




"""  
print("this module is in name: '" + __name__ + "'")
if __name__ == "__main__":
    csv = "relational_publication.csv"
    jsn = "relational_other_data.json"
    path = "./relational_db/"
    Dataobject = Data(path, csv, jsn)
    #print(Dataobject.Cites_DF.head(5))


"""

csv1 = "./graph_db/graph_publications.csv"


#dataObj = extraclasses.DataCSV(csv1)
#print(dataObj.Publication_DF.info())

"""
csv1 = "./graph_db/graph_publications.csv"



#dataObj = extraclasses.DataCSV(csv1)
#print(dataObj.Publication_DF.info())

"""
=======
csv1 = "./graph_db/graph_publications.csv"


#dataObj = extraclasses.DataCSV(csv1)
#print(dataObj.Publication_DF.info())
"""
#print(dataObj.Publication_DF.info())
