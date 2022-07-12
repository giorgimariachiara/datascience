
from impl2 import RelationalProcessor, TriplestoreProcessor
from json import load
import pandas as pd


class RelationalDataProcessor(RelationalProcessor):    
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
    

filePath1 = "./relational_db/relational_publication.csv"
filePath2 = "./relational_db/relational_other_data.json"

data = RelationalDataProcessor.uploadData(filePath1)
print(data)


publication_df = pd.read_csv("./relational_db/relational_publication.csv", 
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


print(publication_df)    


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
            print("The file format in imput is not correct!")
from rdflib import URIRef, Namespace
from rdflib import Graph 



if __name__ == "__main__":

    dbo = Namespace("http://dbpedia.org/ontology/")

    # EXAMPLE 3: doing RDFlib triple navigation using SPARQLStore as a Graph()
    print("Triple navigation using SPARQLStore as a Graph():")
    graph = Graph("SPARQLStore", identifier="http://dbpedia.org")
    graph.open("http://dbpedia.org/sparql")
    # we are asking DBPedia for 3 skos:Concept instances
    count = 0
    from rdflib.namespace import RDF, SKOS

    for s in graph.subjects(predicate=RDF.type, object=SKOS.Concept):
        count += 1
        print(f"\t- {s}")
        if count >= 3:
            break