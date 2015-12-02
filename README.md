# ckanext-interlinking

A CKAN extension that allows resource interlinking using recline. 

## Lucene Servlet for interlinking
The lucene servlet is able to interlinking given terms in Greek with refence datasets. The reference datasets must be provisioned in CSV format, and be indexed once prior searching in them. The user has alse to dictate the field which will be indexed.

After indexing datasets search queries can be carried out. Each time the servlet return the 10 best results order by score in descending order. Apart from the mathcing term of the indexed field, the score and the values of the rest of the fields are returned back.

### Indexing datsets
The reference dataset must use commas as delimiter and its first row should carry the names of its columns. The csv must be placed inside ```'WebContent/WEB-INF/data/'``` folder. Then an indexing POST request must be sent on the service (e.g. at ```http://localhost:8080/LuceneInterlinking/Interlinker```) with the follwing JSON body:

```json
{
    "mode": "index",
    "index": <index>,
    "index_field": <index_field>,
    "file": <file>
}
```
```<index>``` is the name of the index which will be created. ```<index_field>``` is the name of the column to be indexed. ```<file>``` is the name of the file to be indexed.

After this step the dataset is available for search queries.

### Querying indexed datasets
There two ways to query an indexed dataset. The first is a stemmed term serch query and the second a wildcard search query.

#### Stemmed term search query
It stems the search term and it uses it to search an index with stemmed values fot the indexed field of the indexed dataset. The request is a POST one, applied on the same URL as the indexing query. The body of the request is as follows:

```json
{
    "mode": "search",
    "term": <term>,
    "reference": <reference>
}
```

```<term>``` is the search term. ```<reference>``` is the name of referenced dataset which will be queried and it essentialy refers to the ```<index>``` name given during indexing.

#### Wildcard search query
This type of query is essentially a wildcard asterisk (*) search where the asterisk is placed at the end of the search term. Thus it searches for results which start with the searching term. The request is a POST one, applied on the same URL as the indexing query. The body of the request is as follows:

```json
{
    "mode": "like",
    "term": <term>,
    "reference": <reference>,
}
```

```<term>``` and ```<reference>``` have the same significance as in stemmed term search query.

## Configuration file settings for ckanext-interlinking

First, add `recline_interlinking` to the list of enabled CKAN plugins (`ckan.plugins`). 

The plugin needs two configuration settings:

```conf
# ckanext-interlinking reference resources
ckanext.interlinking.references = 
	Names of Municipalities (Kallikratis plan):kallikratis
	Names of Municipalities (Kapodistrias plan):kapodistrias
	Geonames for Greece:geonames
	
#ckanext-interlinking lucene servlet url
ckanext.interlinking.lucene_url = http://localhost:8080/LuceneInterlinking/Interlinker
```

```ckanext.interlinking.references``` contains an array of inscriptions for every reference dataset indexed in the lucene servlet for interlinking. Each inscription is delimited be the ```:``` character. The first part is the full name of the reference dataset, as it appears in the interlinking UI, while the second is the name of the refeence datset which will be used for search queries. ```ckanext.interlinking.lucene_url``` contains the URL where the lucene servlet is available.
