from implRel import TriplestoreProcessor, DataCSV, DataJSON
import os 
from rdflib import Graph, URIRef, Literal, Namespace


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
                my_graph.add((subj, publicationYear, Literal(row["publication_year"])))