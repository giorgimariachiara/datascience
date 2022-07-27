import csv
from implRel import TriplestoreProcessor, DataCSV, DataJSON
import os 
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from SPARQLWrapper import SPARQLWrapper 


#Namespaces used
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
SCHEMA = Namespace("https://schema.org/")
FABIO = Namespace("http://purl.org/spar/fabio/")
BIBO = Namespace("https://bibliontology.com/")

#dobbiamo definire ogni resource e property con la class URIRef creando URIRef objects 
#CLASSES OF RESOURCES
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Proceedingspaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceeding = URIRef("http://purl.org/ontology/bibo/Proceedings") #sbagliato
Person = URIRef("https://schema.org/Person")
organization = URIRef("https://schema.org/Organization") #qui non so se va bene publisher così perchè il dato che ci da è il crossref 
Publication = URIRef("https://schema.org/CreativeWork")
# attributes related to classes
citing = URIRef("http://purl.org/ontology/bibo/cites")
citation = URIRef("https://schema.org/citation")
author = URIRef("https://schema.org/author")
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
familyName = URIRef("https://schema.org/familyName")
givenName = URIRef("https://schema.org/givenName") 
name = URIRef("https://schema.org/name")
chapter = URIRef("https://schema.org/Chapter")
event = URIRef("https://schema.org/Event")
publisher = URIRef("https://schema.org/publisher")
issn_isbn = URIRef("http://gbol.life/ontology/bibo/identifier/")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")



class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            CSV_Rdata = DataCSV(path)

            base_url = "https://github.com/giorgimariachiara/datascience/res/"

            my_graph = Graph()

            for idx, row in CSV_Rdata.Publication_DF.iterrows():  
                subj = URIRef(base_url + row["id"]) 

                my_graph.add((subj, RDF.type, Publication))
                my_graph.add((subj, title, Literal(row["title"])))    
                my_graph.add((subj, identifier, Literal(row["id"])))
                my_graph.add((subj, publicationYear, Literal(row["publicationYear"])))
                my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
                my_graph.add((subj, publisher, URIRef(base_url + row["publisher"]))) 

                if row["type"] == "journal-article":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, JournalArticle)) 
                elif row["type"] == "book-chapter":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, BookChapter))
                else: 
                    if row["type"]== "proceedings-paper":
                        if row["type"] != "":
                            my_graph.add((subj, RDF.type, Proceedingspaper)) #ho aggiunto anche il controllo e procpaper perchè sennò le avrebbe aggiunte alle caselle vuote 
                
                if row["venue_type"] == "journal":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, Journal))
                elif row["venue_type"] == "book":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, Book))
                else: 
                    if row["type"]== "proceedings":
                        if row["type"] != "":
                            my_graph.add((subj, RDF.type, Proceeding))

            for idx, row in CSV_Rdata.Journal_article_DF.iterrows():
                subj = URIRef(base_url + row["id"])
                
                my_graph.add((subj, RDF.type, JournalArticle)) 
                if row["issue"] != "":
                    my_graph.add((subj, issue, Literal(row["issue"])))
                if row["volume"] != "":
                    my_graph.add((subj, volume, Literal(row["volume"])))
            
            for idx, row in CSV_Rdata.Book_chapter_DF.iterrows():
                subj = URIRef(base_url + row["id"])
                my_graph.add((subj, RDF.type, BookChapter))
                if row["chapter"] != "": 
                    my_graph.add((subj, chapter, Literal(row["chapter"])))
            
            for idx, row in CSV_Rdata.Proceedings_paper_DF.iterrows():
                subj = URIRef(base_url + row["id"])
                my_graph.add((subj, RDF.type, Proceedingspaper))

        
            for idx, row in CSV_Rdata.Proceedings_DF.iterrows():
                
                my_graph.add((subj, RDF.type, Proceeding))
                if row["event"] != "":  
                        my_graph.add((subj, event, Literal(row["event"])))
                
            self.my_graph= my_graph

            store = SPARQLUpdateStore()
            # The URL of the SPARQL endpoint is the same URL of the Blazegraph
            # instance + '/sparql'
            endpointUrl = self.getEndpointUrl()

            # It opens the connection with the SPARQL endpoint instance
            store.open((endpointUrl, endpointUrl))

            for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
                store.add(triple)   
            # Once finished, remeber to close the connection
            store.close()
            
        elif f_ext.upper() == ".JSON":
            JSN_Rdata = DataJSON(path)

            base_url = "https://github.com/giorgimariachiara/datascience/res/"

            my_graph = Graph()

            for idx, row in JSN_Rdata.Organization_DF.iterrows():
                subj = URIRef(base_url + row["id"])
            
                my_graph.add((subj, RDF.type, organization))
                my_graph.add((subj, name, Literal(row["name"])))  
                my_graph.add((subj, identifier, Literal(row["id"])))

            for idx, row in JSN_Rdata.Cites_DF.iterrows():
                subj = URIRef(base_url + row["citing"])

                if row["cited"] != None:
                    my_graph.add((subj, citation, URIRef(base_url + str(row["cited"]))))
            
            for idx, row in JSN_Rdata.VenuesEXT_DF.iterrows():
                subjvenuext = URIRef(base_url + row["id"])
                
                my_graph.add(subjvenuext, identifier, Literal(row["id"])) #questo va bene identifier? 
            
            for idx, row in JSN_Rdata.VenuesId_DF.iterrows():
                subj = URIRef(base_url + row["doi"])
                
                my_graph.add(subj, publicationVenue, subjvenuext) #ma qui va bene ? si ripeterà o no? 
                my_graph.add((subj, issn_isbn, Literal(row["issn_isbn"])))    #QUESTO È STATO CAMBIATO 

            for idx, row in JSN_Rdata.Person_DF.iterrows():
                subjperson = URIRef(base_url + row["orc_id"]) 
            
                my_graph.add((subjperson, RDF.type, Person))
                my_graph.add((subjperson, givenName, Literal(row["given_name"])))
                my_graph.add((subjperson, familyName, Literal(row["family_name"])))
                my_graph.add((subjperson, identifier, Literal(row["orc_id"])))

            for idx, row in JSN_Rdata.Author_DF.iterrows(): 
                subj = URIRef(base_url + row["doi"])
                 
                my_graph.add(subj, author, subjperson) #ho aggiunto questa 
                #my_graph.add((subj, author, URIRef(base_url + row["orc_id"])))

            self.my_graph = my_graph

            store = SPARQLUpdateStore()
            # The URL of the SPARQL endpoint is the same URL of the Blazegraph
            # instance + '/sparql'
            #endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'
            endpointUrl = self.getEndpointUrl()

            # It opens the connection with the SPARQL endpoint instance
            store.open((endpointUrl, endpointUrl))

            for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
                store.add(triple)   
            # Once finished, remeber to close the connection
            store.close()

        else:
            print("problem!!")
            return False

        return True

