#!/usr/bin/env python
# -*- coding: utf-8 -*-

import solr
from random import uniform
import pprint


def spell_search(term, reference_resource):
    #TODO use reference_resource
    suggestions = []
    suggested_terms = []
    found_terms = []
    if term != '':
        
        conn = solr.SolrConnection('http://127.0.0.1:8080/solr/cities2')
        
        # First have a look if the term already exists
        response = conn.select('term:'+term)

        for i in range(len(response)):
            for key in response.results[i]:
                if(key == 'term'):
                    found_terms.append(response.results[i][key])
        
        # Then use spellcheck to search results with some distance from the original term
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
                    suggested_terms.append(suggestion['word'])
            
        conn.close()
        
        
        if len(found_terms) > 0:
            for term in found_terms:
                suggestion = {'term': term, 'score': '1'}
                suggestions.append(suggestion)
                
        for term in suggested_terms:
            suggestion = {'term': term, 'score': str(uniform(0,1))}
            suggestions.append(suggestion)
        
    return suggestions