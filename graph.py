from extraclasses import  DataCSV, DataJSON
import os 
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from impl import TriplestoreProcessor

"""
#dobbiamo definire ogni resource e property con la class URIRef creando URIRef objects 
#CLASSES OF RESOURCES
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Proceedingspaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceeding = URIRef("http://purl.org/ontology/bibo/Proceedings") #sbagliato
Person = URIRef("https://schema.org/Person")
Organization = URIRef("https://schema.org/Organization") #qui non so se va bene publisher così perchè il dato che ci da è il crossref 
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
event = URIRef("https://schema.org/event") 
publisher = URIRef("https://schema.org/publisher")
issn_isbn = URIRef("http://gbol.life/ontology/bibo/identifier/")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
"""


class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path):
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            CSV_Rdata = DataCSV(path)

            base_url = "https://github.com/giorgimariachiara/datascience/res/"

            JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
            BookChapter = URIRef("https://schema.org/Chapter")
            Proceedingspaper = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")
            Journal = URIRef("https://schema.org/Periodical")
            Book = URIRef("https://schema.org/Book")
            Proceeding = URIRef("http://purl.org/ontology/bibo/Proceedings") 
            Publication = URIRef("https://schema.org/CreativeWork")


            publicationYear = URIRef("https://schema.org/datePublished")
            title = URIRef("https://schema.org/name")
            issue = URIRef("https://schema.org/issueNumber")
            volume = URIRef("https://schema.org/volumeNumber")
            identifier = URIRef("https://schema.org/identifier")
            chapter = URIRef("https://schema.org/Chapter")
            event = URIRef("https://schema.org/event") 
            publisher = URIRef("https://schema.org/publisher")
            name = URIRef("https://schema.org/name")

            publicationVenue = URIRef("https://schema.org/isPartOf")

            my_graph = Graph()

            for idx, row in CSV_Rdata.Publication_DF.iterrows():  
                subj = URIRef(base_url + row["id"]) 

                my_graph.add((subj, RDF.type, Publication))
                if row["title"] != "":
                    my_graph.add((subj, title, Literal(row["title"])))
                if row["id"] != "":    
                    my_graph.add((subj, identifier, Literal(row["id"])))
                if row["publicationYear"] != "":
                    my_graph.add((subj, publicationYear, Literal(row["publicationYear"])))
                if row["publication_venue"] != "":
                    my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
                if row["publisher"] != "":
                    my_graph.add((subj, publisher, URIRef(base_url + row["publisher"]))) 

                if row["type"] == "journal-article":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, JournalArticle)) 
                elif row["type"] == "book-chapter":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, BookChapter))
                elif row["type"]== "proceedings-paper":
                    if row["type"] != "":
                        my_graph.add((subj, RDF.type, Proceedingspaper)) 
                else: 
                    print("WARNING: Unrecognized publication type!")
                
            #triple publications type

            for idx, row in CSV_Rdata.Journal_article_DF.iterrows():
                subj = URIRef(base_url + row["id"])
 
                if row["issue"] != "":
                    my_graph.add((subj, issue, Literal(row["issue"])))
                if row["volume"] != "":
                    my_graph.add((subj, volume, Literal(row["volume"])))
            
            for idx, row in CSV_Rdata.Book_chapter_DF.iterrows():
                subj = URIRef(base_url + row["id"])
                
                if row["chapter"] != "": 
                    my_graph.add((subj, chapter, Literal(row["chapter"])))
            
            #triple Venue type 
            
            for idx, row in CSV_Rdata.Book_DF.iterrows():
                local_id = "book-" + str(idx)
                subj = URIRef(base_url + local_id)
                my_graph.add((subj, name, Literal(row["publication_venue"])))
                my_graph.add((subj, RDF.type, Book))

            for idx, row in CSV_Rdata.Journal_DF.iterrows():
                local_id = "journal-" + str(idx)
                subj = URIRef(base_url + local_id)
                my_graph.add((subj, name, Literal(row["publication_venue"])))
                my_graph.add((subj, RDF.type, Journal))
            
            for idx, row in CSV_Rdata.Proceedings_DF.iterrows():
                local_id = "proceeding-" + str(idx)
                subj = URIRef(base_url + local_id) 
                my_graph.add((subj, RDF.type, Proceeding))
                if row["publication_venue"] != "":
                    my_graph.add((subj, name, Literal(row["publication_venue"])))
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

            Person = URIRef("https://schema.org/Person")
            Organization = URIRef("https://schema.org/Organization")

            identifier = URIRef("https://schema.org/identifier")
            familyName = URIRef("https://schema.org/familyName")
            givenName = URIRef("https://schema.org/givenName") 
            name = URIRef("https://schema.org/name")
            citation = URIRef("https://schema.org/citation")
            author = URIRef("https://schema.org/author")
            issn_isbn = URIRef("http://purl.org/dc/terms/identifier")


            my_graph = Graph()

            for idx, row in JSN_Rdata.Organization_DF.iterrows():
                subj = URIRef(base_url + row["id"])

                my_graph.add((subj, RDF.type, Organization))
                my_graph.add((subj, name, Literal(row["name"])))  
                my_graph.add((subj, identifier, Literal(row["id"])))

            for idx, row in JSN_Rdata.Cites_DF.iterrows():
                subj = URIRef(base_url + row["citing"])

                if row["cited"] != None:
                    my_graph.add((subj, citation, URIRef(base_url + str(row["cited"]))))
            
            for idx, row in JSN_Rdata.VenuesId_DF.iterrows():
                subj = URIRef(base_url + row["doi"])
                
                my_graph.add((subj, issn_isbn, Literal(row["issn_isbn"])))    

            for idx, row in JSN_Rdata.Person_DF.iterrows():
                subjperson = URIRef(base_url + row["orc_id"]) 
            
                my_graph.add((subjperson, RDF.type, Person))
                my_graph.add((subjperson, givenName, Literal(row["given_name"])))
                my_graph.add((subjperson, familyName, Literal(row["family_name"])))
                my_graph.add((subjperson, identifier, Literal(row["orc_id"])))

            for idx, row in JSN_Rdata.Author_DF.iterrows():     
                
                my_graph.add(((URIRef(base_url + row["orc_id"]), author, URIRef(base_url + row["doi"]))))

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

