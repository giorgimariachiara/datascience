from rdflib import Graph 
from rdflib import URIRef
from rdflib import Literal 
from pandas import read_csv, Series
from rdflib import RDF

my_graph = Graph() #empty rdf graph 

#dobbiamo definire ogni resource e property con la class URIRef creando URIRef objects 
#CLASSES OF RESOURCES
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Proceedingspaper = URIRef("https://schema.org/ScholarlyArticle")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceeding = URIRef("https://schema.org/Event")

# attributes related to classes
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
name = URIRef("https://schema.org/name")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://github.com/giorgimariachiara/datascience/res/"

publications = read_csv("./graph.db/graph_publications.csv", 
                  keep_default_na=False,
                  dtype={
                      "id": "string",
                      "title": "string",
                      "type": "string",
                      "publication_year":"string",
                      "issue":"string",
                      "volume":"string",
                      "chapter":"string",
                      "publication_venue":"string",
                      "venue_type": "string",
                      "publisher": "string",
                      "event":"string"
                  })

publications_internal_id = {}
for idx, row in publications.iterrows():
    internal_id = "local-" + str(idx)

# The shape of the new resources that are venues is
    # 'https://comp-data.github.io/res/venue-<integer>'
    subj = URIRef(base_url + internal_id)

    # We put the new venue resources created here, to use them
    # when creating publications
    publications_internal_id[row["id"]] = subj

    if row["type"] == "journal-article":
       my_graph.add((subj, RDF.type, JournalArticle)) 

       #questi sono solo per i journalarticles
       my_graph.add((subj, issue, Literal(row["issue"])))
       my_graph.add((subj, volume, Literal(row["volume"])))
    else:
        my_graph.add((subj, RDF.type, BookChapter))

        #questi solo per i book chapters
my_graph.add((subj, name, Literal(row["title"])))
my_graph.add((subj, identifier, Literal(row["doi"])))

    
    #qui non so se dobbiamo mettere un elif per proceedings
my_graph.add((subj, ))
