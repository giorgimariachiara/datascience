from mainRel import RelationalDataProcessor
from implRel import GenericQueryProcessor, RelationalQueryProcessor


jsn0 = "./relational_db/relational_other_data.json"
csv = "./relational_db/relational_publication.csv"
dbpath0 = "publication.db"
"""
obj = RelationalDataProcessor() 
obj.setDbPath(dbpath0) # primo setting del path al db per caricamento dati
obj.uploadData(jsn0)
obj.uploadData(csv)
"""
rqp = RelationalQueryProcessor()
rqp.setDbPath(dbpath0)
#print(rqp.getPublicationsPublishedInYear(2020))
gqp = GenericQueryProcessor()
gqp.addQueryProcessor(rqp)
resultq1 = gqp.getPublicationsPublishedInYear(2020)
for el in resultq1:
    print(el.__str__())


#obj.uploadData(jsn)
    # do the same for triplestore db
#rqp = RelationalQueryProcessor()
#rqp.setDbPath(path) # secondo setting del path al db per le queries
#gqp = GenericQueryProcessor()
#gqp.addQueryProcessor(rqp)
    
#print(gqp.getPublicationsPublishedInYear(2020))