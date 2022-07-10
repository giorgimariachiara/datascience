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

organization_df.insert(0, 'GOrganizationId', range(0, organization_df.shape[0]))
organization_df['GOrganizationId']= organization_df['GOrganizationId'].apply(lambda x: 'organization-'+ str(int(x)))

for idx, row in organization_df.iterrows():
    subj = URIRef(base_url + row["GOrganizationId"])
    
    my_graph.add((subj, RDF.type, organization))
    my_graph.add((subj, name, Literal(row["name"])))
    my_graph.add((subj, identifier, Literal(row["id"])))
    
pvdataframe = publications[["publication_venue", "venue_type", "publisher"]].drop_duplicates()
pvdataframe.insert(0, 'VenueId', range(0, pvdataframe.shape[0]))
pvdataframe['VenueId']= pvdataframe['VenueId'].apply(lambda x: 'venue-'+ str(int(x)))
venuesdataframe = pd.merge(pvdataframe, organization_df, left_on="publisher", right_on="id")
#print(venuesdataframe.head(5))
for idx, row in venuesdataframe.iterrows():
    subj = URIRef(base_url + row["VenueId"])
    
    
    my_graph.add((subj, title, Literal(row["publication_venue"])))
    my_graph.add((subj, RDF.type, Literal(row["venue_type"])))
    my_graph.add((subj, publisher, URIRef(base_url + row["GOrganizationId"]))) 

print(my_graph.print())