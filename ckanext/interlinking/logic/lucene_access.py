import pprint
import requests
import json

def search(term, reference_resource, mode):
    suggestions = {}
    suggestions['fields'] = []
    suggestions['records'] = []
    
    if term != '':   
        lucene_url = "http://localhost:8080/LuceneInterlinking/Interlinker"
        data = {"reference": reference_resource,
                "term": term,
                "mode": mode}
        data = json.dumps(data)
        res = requests.post(lucene_url, data)
        results = res.json()
                
        for field in results['fields']:
            suggestions['fields'].append(field)
        for record in results['records']:
            suggestions['records'].append(record)
            
    return suggestions


def getFields (reference_resource, originals):
    lucene_url = "http://localhost:8080/LuceneInterlinking/Interlinker"
    if originals:
        data = {"index": reference_resource,
                    "mode": "fields"}
    else:
        data = {"index": reference_resource,
                    "mode": "fields",
                    "originals": "false"}
        
    data = json.dumps(data)
    res = requests.post(lucene_url, data)
    results = res.json()
    return results['fields']
        
        
    