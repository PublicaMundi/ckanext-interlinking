import pprint
import requests
import json
import pylons


def search(term, reference_resource, mode):
    suggestions = {}
    suggestions['fields'] = []
    suggestions['records'] = []
    if term != '':   
        lucene_url = pylons.config.get('ckanext.interlinking.lucene_url')
        data = {"reference": reference_resource,
                "term": term,
                "mode": mode}
        data = json.dumps(data)
        try:
            res = requests.post(lucene_url, data)
        except:
            return -1
        if res.status_code != requests.codes.ok:
            return -1
        
        results = res.json()
        for field in results['fields']:
            suggestions['fields'].append(field)
        for record in results['records']:
            suggestions['records'].append(record)
    return suggestions


def getFields (reference_resource, originals):
    lucene_url = pylons.config.get('ckanext.interlinking.lucene_url')
    if originals:
        data = {"index": reference_resource,
                    "mode": "fields"}
    else:
        data = {"index": reference_resource,
                    "mode": "fields",
                    "originals": "false"}
        
    data = json.dumps(data)
    try:
        res = requests.post(lucene_url, data)
    except:
        return -2
    if res.status_code != requests.codes.ok:
        return -1
    results = res.json()
    return results['fields']
    