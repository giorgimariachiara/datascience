from implRel import GenericQueryProcessor, RelationalQueryProcessor, RelationalDataProcessor


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
#print(rqp.getVenuesByPublisherId("crossref:78"))
gqp = GenericQueryProcessor()
gqp.addQueryProcessor(rqp)
#print(gqp.getVenuesByPublisherId("crossref:78"))
#print(gqp.getMostCitedPublication())
#print(gqp.getPublicationsByAuthorId("0000-0003-0530-4305"))
print(gqp.getJournalArticlesInJournal("issn:0138-9130"))
#for el in resultq1:
    #print(el.__str__())


#obj.uploadData(jsn)
    # do the same for triplestore db
#rqp = RelationalQueryProcessor()
#rqp.setDbPath(path) # secondo setting del path al db per le queries
#gqp = GenericQueryProcessor()
#gqp.addQueryProcessor(rqp)
    
#print(gqp.getPublicationsPublishedInYear(2020))

"""       
print("this module is in name: '" + __name__ + "'")
if __name__ == "__main__":
    csv = "relational_publication.csv"
    jsn = "relational_other_data.json"
    path = "./relational_db/"
    Dataobject = Data(path, csv, jsn)
    #print(Dataobject.Cites_DF.head(5))

"""