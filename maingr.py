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
from impl2 import Organization
from json import load
import pandas as pd 
from pandas import DataFrame

my_graph = Graph() #empty rdf graph 


my_graph = Graph() #empty rdf graph 

#Namespaces used
SCHEMA = Namespace("https://schema.org/")


#dobbiamo definire ogni resource e property con la class URIRef creando URIRef objects 
#CLASSES OF RESOURCES
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Proceedingspaper = URIRef("https://schema.org/ScholarlyArticle")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")
Proceeding = URIRef("https://schema.org/Event")

# attributes related to classes
citation = URIRef("https://schema.org/citation")
author = URIRef("https://schema.org/author")
doi = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/name")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
identifier = URIRef("https://schema.org/identifier")
familyname = URIRef("https://schema.org/familyName")
name = URIRef("https://schema.org/name") #non so se serve 
chapter = URIRef("https://schema.org/Chapter")
organization = URIRef("https://schema.org/Organization") #qui non so se va bene publisher così perchè il dato che ci da è il crossref 
event = URIRef("https://schema.org/Event")
publisher = URIRef("https://schema.org/publisher")

# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")

# This is the string defining the base URL used to defined
# the URLs of all the resources created from the data
base_url = "https://github.com/giorgimariachiara/datascience/res/"

publications = read_csv("graph_db/graph_publications.csv", 
                keep_default_na= False,
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

with open("graph_db/graph_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)
    
#organization dataframe 

crossref = json_doc.get("publishers")
id_and_name = crossref.values()
organization_df = pd.DataFrame(id_and_name)

organization_df.insert(0, 'InternalId', range(0, organization_df.shape[0]))
organization_df['id']= organization_df['id'].apply(lambda x: 'organization-'+ str(int(x)))

for idx, row in organization_df.iterrows():
    subj = URIRef(base_url + row["id"])
    
    my_graph.add((subj, RDF.type, organization))
    my_graph.add((subj, name, Literal(row["name"])))
    my_graph.add((subj, identifier, Literal(row["InternalId"])))

pvdataframe = publications[["publication_venue", "venue_type", "publisher"]].drop_duplicates()
pvdataframe.insert(0, 'id', range(0, pvdataframe.shape[0]))
pvdataframe['id']= pvdataframe['id'].apply(lambda x: 'venue-'+ str(int(x)))
print(pvdataframe.head(5))

for idx, row in pvdataframe.iterrows():
    subj = URIRef(base_url + row["id"])
    
    my_graph.add((subj, title, Literal(row["publication_venue"])))
    my_graph.add((subj, RDF.type, Literal(row["venue_type"])))
    my_graph.add((subj, publisher, Literal(row["InternalId"]))) #qui bisogna mettere l'id del dataframe di organization 
    


   
""" 

authors = json_doc["authors"]
author_df=pd.DataFrame(authors.items(),columns=['doi','author']).explode('author')

author_df=pd.json_normalize(json.loads(author_df.to_json(orient="records")))
author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)

author_df=pd.DataFrame(author_df)


with open("graph_db/graph_other_data.json", "r", encoding="utf-8") as f:
    json_doc = load(f)
    """


# The shape of the new resources that are venues is
    # 'https://comp-data.github.io/res/venue-<integer>'
    

    # We put the new venue resources created here, to use them
    # when creating publications
    #publications_internal_id[row["id"]] = subj

    #print(len(publications_internal_id))



"""
author = json_doc.get("authors")
authordict = author.values()

for key in dict:
    if authordict[key] == "family":
        my_graph.add((subj, familyname, authordict[key]))

print(len(my_graph))


#print(len(my_graph))


publications_internal_id = {}
for idx, row in publications.iterrows():
    internal_id = "publication-" + str(idx)
    subj = URIRef(base_url + internal_id)

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

    if row["venue_type"] == "book":
        if row["venue_type"] != "":
            my_graph.add((subj, RDF.type, Book))
    elif row["venue_type"] == "journal":
        if row["venue_type"] != "":
            my_graph.add((subj, RDF.type, Journal))
    else:
        if row["venue_type"] == "proceeding":
            my_graph.add((subj, RDF.type, Proceeding))
    
    if row["event"] != "":  
        my_graph.add((subj, event, Literal(row["event"])))

    if row["publisher"] != "":
        my_graph.add((subj, organization, Literal(row["publisher"]))) 

    if row["publication_venue"] != "":
        my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))   #venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 

    

    my_graph.add((subj, title, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["id"])))
    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
        
    
    
#venue_internal_id[row["publication venue"]] questo è quello che ha mesos Peroni bisogna ccapire perchè 

#add data to the database
#store = SPARQLUpdateStore()

# The URL of the SPARQL endpoint is the same URL of the Blazegraph
# instance + '/sparql'
endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

# It opens the connection with the SPARQL endpoint instance
store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)): #none none none means that it should consider all the triples of the graph 
    store.add(triple)   
# Once finished, remeber to close the connection
store.close()

 """


print(len(my_graph))


