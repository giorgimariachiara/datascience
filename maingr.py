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
from pandas import DataFrame
#from impl2 import TriplestoreProcessor

my_graph = Graph() #empty rdf graph 


my_graph = Graph() #empty rdf graph 

#Namespaces used
SCHEMA = Namespace("https://schema.org/")
FABIO = Namespace("http://purl.org/spar/fabio/")
BIBO = Namespace("https://bibliontology.com/")

my_graph.bind('schema', SCHEMA)
my_graph.bind('fabio', FABIO)
my_graph.bind('bibo', BIBO)

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
csv_path = "./relational_db/relational_publication.csv"
json_path = "./relational_db/relational_other_data.json"



class TriplestoreDataProcessor(TriplestoreProcessor):
    
    def uploadData(data_path):
        data_path_string = str(data_path)
        if data_path_string.endswith(".csv"):
            csv_data = pd.read_csv(data_path,
                        dtype={
                                    "id": "string",
                                    "title": "string",
                                    "type": "string",
                                    "publication_year": "string",
                                    "issue": "string",
                                    "volume": "string",
                                    "chapter": "string",
                                    "publication_venue": "string",
                                    "venue_type": "string",
                                    "publisher": "string",
                                    "event": "string"

                        },encoding="utf-8")
            return csv_data
        elif  data_path_string.endswith(".json"):
            with open(data_path, "r", encoding="utf-8") as f:
                json_data = load(f)
            return json_data
        else:
            print("The file format in input is not correct!")
            
publications = TriplestoreDataProcessor.uploadData(csv_path)      
json_doc = TriplestoreDataProcessor.uploadData(json_path)      
"""
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
organization_df = organization_df.rename(columns={"id":"crossref"})


organization_df.insert(0, 'GOrganizationId', range(0, organization_df.shape[0]))
organization_df['GOrganizationId']= organization_df['GOrganizationId'].apply(lambda x: 'organization-'+ str(int(x)))

for idx, row in organization_df.iterrows():
    subj = URIRef(base_url + row["GOrganizationId"])
    
    my_graph.add((subj, RDF.type, organization))
    my_graph.add((subj, SCHEMA["name"], Literal(row["name"])))   #NON SAPPIAMO SE VA FATTO O NO 
    my_graph.add((subj, identifier, Literal(row["crossref"])))
    
pvdataframe = publications[["publication_venue", "venue_type", "publisher"]].drop_duplicates()
pvdataframe.insert(0, 'VenueId', range(0, pvdataframe.shape[0]))
pvdataframe['VenueId']= pvdataframe['VenueId'].apply(lambda x: 'venue-'+ str(int(x)))
venuesdataframe = pd.merge(pvdataframe, organization_df, left_on="publisher", right_on="crossref")

#print(venuesdataframe.head(5))
for idx, row in venuesdataframe.iterrows():
    subj = URIRef(base_url + row["VenueId"])
    
    my_graph.add((subj, title, Literal(row["publication_venue"])))
    my_graph.add((subj, RDF.type, Literal(row["venue_type"])))
    my_graph.add((subj, publisher, URIRef(base_url + row["GOrganizationId"])))  


persons=json_doc["authors"]

doi_l = []
name_orcid_l = []

for key in persons:
    for item in persons[key]:
        doi_l.append(key)
        name_orcid_l.append(item)

person_l = []

for item in name_orcid_l:
    if item not in person_l:
        person_l.append(item)

family_names_l = []
for item in person_l:
   family_names_l.append(item.get("family"))


given_names_l = []
for item in person_l:
    given_names_l.append(item.get("given"))


orcid_l = []
for item in person_l:
    orcid_l.append(item.get("orcid"))

person_df = pd.DataFrame({
    "orcid": Series(orcid_l, dtype="string", name="orc_id"),
    "given": Series(given_names_l, dtype="string", name="given_name"),
    "family": Series(family_names_l, dtype="string", name="family_name"),
})

for idx, row in person_df.iterrows():
    subj = URIRef(base_url + row["orcid"])

    my_graph.add((subj, RDF.type, Person))
    my_graph.add((subj, givenName, Literal(row["given"])))
    my_graph.add((subj, familyName, Literal(row["family"])))
    my_graph.add((subj, identifier, Literal(row["orcid"])))
    


#creiamo il dataframe author con doi e orcid
#author dovrebbe avere un internal id? nel relational non ce l'ha ma qual è quindi la sua primary key? 
authors = json_doc["authors"]
author_df=pd.DataFrame(authors.items(),columns=['doi','author']).explode('author')
author_df=pd.json_normalize(json.loads(author_df.to_json(orient="records")))
author_df.rename(columns={"author.family":"family_name","author.given":"given_name","author.orcid":"orc_id"}, inplace = True)
author_df.drop("family_name", axis=1, inplace = True)
author_df.drop("given_name", axis =1, inplace = True)

for idx, row in author_df.iterrows(): 
    subj = URIRef(base_url + row["orc_id"])

   # my_graph.add((subj, RDF.type, author))
   # my_graph.add((subj, identifier, Literal(row["orc_id"])))
    my_graph.add((subj, author, URIRef(base_url + row["doi"])))


dfPublicationVenue = pd.merge(publications, venuesdataframe, left_on="publication_venue", right_on="publication_venue")

nomi = []
for column in dfPublicationVenue:
    nomi.append(column)
print(nomi)
  
#print(dfPublicationVenue[["venue_type_x", "venue_type_y"]])  


for idx, row in dfPublicationVenue.iterrows(): #qui l'iterrows va fatto su dfPublicationVenue? 
    subj = URIRef(base_url + row["id"])

   # if row["publication_venue"] != "":
    my_graph.add((subj, publicationVenue, URIRef(base_url + row["VenueId"])))  
    

    my_graph.add((subj, title, Literal(row["title"])))
    my_graph.add((subj, identifier, Literal(row["id"])))
    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))

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

References = json_doc["references"]
cites_df=pd.DataFrame(References.items(),columns=['citing','cited']).explode('cited')
cites_df=pd.json_normalize(json.loads(cites_df.to_json(orient="records")))
cites_df.rename(columns={"References.keys()":"citing","References.values()":"cited"}, inplace = True)

cites_df=pd.DataFrame(cites_df)

for idx, row in cites_df.iterrows():
    subj = URIRef(base_url + row["citing"])

    if row["cited"] != None:

        my_graph.add((subj, SCHEMA["citation"], Literal(row["cited"])))

    #my_graph.add((subj, BIBO["citing"], Literal(row["citing"])))


Venue=json_doc["venues_id"]

doi_list = []
issn_isbn_l = []

for key in Venue:
    for item in Venue[key]:
        doi_list.append(key)
        issn_isbn_l.append(item)

venues = pd.DataFrame({
    "doi": Series(doi_list, dtype="string", name="doi"),
    "issn_isbn": Series(issn_isbn_l, dtype="string", name="issn_isbn"),
    
})

venue_ext_df = pd.merge(dfPublicationVenue, venues, left_on="id", right_on="doi")
venue_ext_df = venue_ext_df[["VenueId", "issn_isbn"]]
venue_ext_df.drop_duplicates(subset= ["VenueId", "issn_isbn"], inplace = True)

for idx, row in venue_ext_df.iterrows():
    subj = URIRef(base_url + row["VenueId"]) #è così? 

    my_graph.add((subj, identifier, Literal(row["issn_isbn"]) )) #è giuto identifier? 

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



#print(my_graph.print())
print(pvdataframe)

