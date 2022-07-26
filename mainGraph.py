from ast import For
from os import urandom
from platform import mac_ver
from platformdirs import user_data_dir
from rdflib import Graph 
from rdflib import URIRef, Namespace
from rdflib import Literal 
import json
import pandas as pd
from pandas import read_csv, Series, read_json
from rdflib import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
import pandas as pd 
from implRel import TriplestoreProcessor
from extraclasses import DataCSV, DataJSON
import os 

"""
#Namespaces used
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

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://github.com/giorgimariachiara/datascience/res/"

"""

csv = "./graph_db/graph_publications.csv"
jsonf = "./graph_db/graph_other_data.json"

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()
     #bisogna aggiungere def init-- o no ? 
    def uploadData(self, path):
        f_ext = os.path.splitext(path)[1]
        if f_ext.upper() == ".CSV":
            CSV_Rdata = DataCSV(path, csv)

        #Namespaces used
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

        # relations among classes
        publicationVenue = URIRef("https://schema.org/isPartOf")

        # This is the string defining the base URL used to defined
        # the URLs of all the resources created from the data
        base_url = "https://github.com/giorgimariachiara/datascience/res/"
        

        my_graph = Graph()

        my_graph.bind('schema', SCHEMA)
        my_graph.bind('fabio', FABIO)
        my_graph.bind('bibo', BIBO)

        for idx, row in CSV_Rdata.Publication_DF.iterrows():  
            subj = URIRef(base_url + row["id"])

            # if row["publication_venue"] != "":
            my_graph.add((subj, publicationVenue, URIRef(base_url + row["VenueId"])))  #questo non c'è più
        
            my_graph.add((subj, RDF.type, Publication))
            my_graph.add((subj, title, Literal(row["title"])))
            my_graph.add((subj, identifier, Literal(row["id"])))
            my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
            #print(row["id"] + " - " + row["issue"])
            

            if row["type"] == "journal-article":
                if row["type"] != "":
                    my_graph.add((subj, RDF.type, JournalArticle)) 
                if row["issue"] != "":
                    my_graph.add((subj, issue, Literal(row["issue"])))
                if row["volume"] != "":
                    my_graph.add((subj, volume, Literal(row["volume"])))

            elif row["type"] == "book-chapter":
                if row["type"] != "":
                    my_graph.add((subj, RDF.type, BookChapter))
                    my_graph.add((subj, chapter, Literal(row["chapter"])))
            else: 
                if row["type"] == "proceeding-paper":
                    my_graph.add((subj, RDF.type, Proceedingspaper))
            
            if row["venue_type_x"] == "book":
                if row["venue_type_x"] != "":
                    my_graph.add((subj, RDF.type, Book))
            elif row["venue_type_x"] == "journal":
                if row["venue_type_x"] != "":
                    my_graph.add((subj, RDF.type, Journal))
            else:
                if row["venue_type_x"] == "proceeding":
                    my_graph.add((subj, RDF.type, Proceeding))
        
                if row["event"] != "":  
                    my_graph.add((subj, event, Literal(row["event"])))


elif f_ext.upper() == ".JSON":

    JSN_Rdata = DataJSON(path, jsonf)



            for idx, row in JSN_Rdata.Organization_DF.iterrows():
                subj = URIRef(base_url + row["GOrganizationId"])
        
            my_graph.add((subj, RDF.type, organization))
            my_graph.add((subj, name, Literal(row["name"])))   #NON SAPPIAMO SE VA FATTO O NO 
            my_graph.add((subj, identifier, Literal(row["crossref"])))


            for idx, row in JSN_Rdata.Cites_DF.iterrows():
                subj = URIRef(base_url + row["citing"])

            if row["cited"] != None:
                my_graph.add((subj, citation, URIRef(base_url + str(row["cited"]))))

            for idx, row in JSN_Rdata.VenuesExt_DF.iterrows():
                subj = URIRef(base_url + row["VenueId"]) 

            my_graph.add((subj, identifier, Literal(row["issn_isbn"]) ))

            for idx, row in JSN_Rdata.Person_DF.iterrows():
                subj = URIRef(base_url + row["orcid"])

            my_graph.add((subj, RDF.type, Person))
            my_graph.add((subj, givenName, Literal(row["given"])))
            my_graph.add((subj, familyName, Literal(row["family"])))
            my_graph.add((subj, identifier, Literal(row["orcid"])))

            for idx, row in JSN_Rdata.Author_DF.iterrows(): 
                subj = URIRef(base_url + row["orc_id"])

            # my_graph.add((subj, RDF.type, author))
            # my_graph.add((subj, identifier, Literal(row["orc_id"])))
            my_graph.add((subj, author, URIRef(base_url + row["doi"])))

        else:
            
            print("problem!!")
            return False

        return True



"""
"""
store = SPARQLUpdateStore()
# The URL of the SPARQL endpoint is the same URL of the Blazegraph
# instance + '/sparql'
endpointUrl = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpointUrl, endpointUrl))

for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
    store.add(triple)   
# Once finished, remeber to close the connection
store.close()

query = 
        SELECT *
        WHERE {
            ?s rdf:type schema:ScholarlyArticle.
            ?s schema:datePublished "2021".
            ?s ?p ?o 
        }       
"""



