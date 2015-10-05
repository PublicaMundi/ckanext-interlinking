#!/usr/bin/env python
# -*- coding: utf-8 -*-

import solr
import pprint


def spell_search(term, reference_resource):
    #TODO use reference_resource
    suggestions = []
    if term != '':
        conn = solr.SolrConnection('http://127.0.0.1:8080/solr/cities2')
        spell = solr.SearchHandler(conn, "/spell")
    
        query = {
            'spellcheck.q' : term,
            'spellcheck' : 'true',
            'spellcheck.collate': 'true',
            'spellcheck.build':'true'
        }
        
        response = spell(**query)
                
        for key in response.spellcheck['suggestions']:
            #print "key>>", key.encode('utf8') 
            if key == term:
                for suggestion in response.spellcheck['suggestions'][key]['suggestion']:
                    suggestions.append(suggestion['word'])
        
    
        conn.close()
    return suggestions