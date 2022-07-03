from rdflib import Graph 
from rdflib import URIRef
from rdflib import Literal 
from pandas import read_csv, Series
from rdflib import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from impl2 import Organization

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
name = URIRef("https://schema.org/name") #non so se serve 
chapter = URIRef("https://schema.org/Chapter")
organization = URIRef("https://schema.org/Organization") #qui non so se va bene publisher così perchè il dato che ci da è il crossref 
event = URIRef("https://schema.org/Event")



# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://github.com/giorgimariachiara/datascience/res/"

publications = read_csv("graph_db/graph_publications.csv", 
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

#publications_internal_id = {}
for idx, row in publications.iterrows():
    internal_id = "local-" + str(idx)

# The shape of the new resources that are venues is
    # 'https://comp-data.github.io/res/venue-<integer>'
    subj = URIRef(base_url + internal_id)

    # We put the new venue resources created here, to use them
    # when creating publications
    #publications_internal_id[row["id"]] = subj

    if row["type"] == "journal-article":
       my_graph.add((subj, RDF.type, JournalArticle)) 

       #questi sono solo per i journalarticles
       my_graph.add((subj, issue, Literal(row["issue"])))
       my_graph.add((subj, volume, Literal(row["volume"])))
    elif row["type"] == 'book-chapter':
        my_graph.add((subj, RDF.type, BookChapter))
        my_graph.add((subj, chapter, Literal(row["chapter"])))
    else: 
        my_graph.add((subj, RDF.type, Proceedingspaper))

    if row["venue_type"] == "book":
        my_graph.add((subj, RDF.type, Book))
    elif row["venue_type"] == "journal":
        my_graph.add((subj, RDF.type, Journal))
    else:
        my_graph.add((subj, RDF.type, Proceeding))
        
my_graph.add((subj, title, Literal(row["title"])))
my_graph.add((subj, identifier, Literal(row["id"])))
my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
my_graph.add((subj, event, Literal(row["event"])))
my_graph.add((subj, organization, Literal(row["publisher"])))

my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))   

#venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 

my_graph.add((subj, publicationVenue, publications_internal_id[row["publication_venue"]]))   #venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 

my_graph.add((subj, publicationVenue, publications_internal_id[row["publication_venue"]]))   #venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 

my_graph.add((subj, publicationVenue, publications_internal_id[row["publication_venue"]]))   #venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 


#add data to the database
store = SPARQLUpdateStore()

# The URL of the SPARQL endpoint is the same URL of the Blazegraph
# instance + '/sparql'
endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
   store.add(triple)
    
# Once finished, remeber to close the connection
store.close()

print(len(my_graph))
