from elasticsearch import Elasticsearch
import csv
import json

# configure elasticsearch

FILE_URL = "data.csv"
ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'esgi'
TYPE_NAME = 'matiere'



class ES:

    def __init__(self, 
                filepath, 
                index_name,
                type_name,
                es_hostname="localhost", 
                es_port=9200):

        self.filepath = filepath
        self.es_host = {"host" : es_hostname, "port" : es_port}
        self.index_name = index_name
        self.type_name =  type_name


    #############################################################
    ########       CREATE ELASTIC-SEARCH INDEX         ##########
    #############################################################

    def load_data_from_file(self):
        data = [] 
        with open(self.filepath, newline='') as csvfile:
            csv_file_object = csv.reader(csvfile, 
                                        delimiter=';', 
                                        quotechar='|')
            header = next(csv_file_object)
            row_number = 0 
            for row in csv_file_object:
                data_dict = {}
                for i in range(len(row)):
                    if header[i] == "nb_heure":
                        data_dict[header[i]] = int(row[i])
                    else:
                        data_dict[header[i]] = row[i]
                op_dict = {
                    "index": {
                        "_index": self.index_name, 
                        "_type": self.type_name, 
                        "_id": str(row_number)
                    }
                }
                data.append(op_dict)
                data.append(data_dict)
                row_number = row_number + 1
        return data

    def create_es_client(self):
        # create ES client, create index
        es = Elasticsearch(hosts = [self.es_host])
        return es

    def create_es_index(self, es):
        if es.indices.exists(self.index_name):
            print("deleting '%s' index..." % (self.index_name))
            res = es.indices.delete(index = self.index_name)
            print(" response: '%s'" % (res))
        # since we are running locally, use one shard and no replicas
        request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }
        print("creating '%s' index..." % (self.index_name))
        res = es.indices.create(index = self.index_name, body = request_body)
        print(" response: '%s'\n\n" % (res))

    def insert_es_data(self, es, data):
        res = es.bulk(index = self.index_name, body = data, refresh = True)
        return res

    def es_search(self, es):
        res = es.search(index = INDEX_NAME, body={"query": {"match_all": {}}})
        print(" response to search: '%s'\n\n" % (res))


    #############################################################
    ########       ELASTIC-SEARCH QUERRIES             ##########
    #############################################################

    def total_hours(self, es):
        res = es.search(index = self.index_name, body={
            "aggs": {
                "nb_heure_total": { "sum": { "field": "nb_heure" } }
            }})
        
        print("Nombre d'heures total: '%s'\n\n" % (res))


    def minimax(self, es, term_type):
        if term_type == "max":
            term = "desc"
            agg_name = "list_heure_max"
        else:
            term = "asc"
            agg_name = "list_heure_min"
        res = es.search(index = self.index_name, body={
        "size": 0,
        "aggs": {
            "group_by_nb_heure": {
            "terms": {
                "field": "nb_heure",
                "order": {
                "_term": term
                },
                "size": 1
            },
            "aggs": {
                agg_name: {
                "top_hits": {
                    "from": 0,
                    "size": 10
                }
                }
            }
            }
        }
        })
        print("Liste heure %s: '%s'\n\n" % (term_type, res))



    def list_min_hour(self, es):
        self.minimax(es, "min")

    def list_max_hour(self, es):
        self.minimax(es, "max")
    

if __name__=="__main__":

    es_object = ES(FILE_URL, INDEX_NAME, TYPE_NAME)
    data = es_object.load_data_from_file()
    es_client = es_object.create_es_client()
    es_object.create_es_index(es_client)
    es_object.insert_es_data(es_client, data)
    es_object.es_search(es_client)
    es_object.total_hours(es_client)
    es_object.list_min_hour(es_client)
    es_object.list_max_hour(es_client)