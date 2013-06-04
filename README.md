Term List Matching Plugin for ElasticSearch
==================================

Similar functionality to Lucene/Solr's TermsComponent.
This Term List plugin will do simple matching against the complete term list found directly in Lucene.

1. Build this plugin:

        mvn compile test package 
        # this will create a file here: target/releases/elasticsearch-term-plugin-1.0-SNAPSHOT.zip
        PLUGIN_PATH=`pwd`/target/releases/elasticsearch-term-plugin-1.0-SNAPSHOT.zip

2. Install the PLUGIN

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -url file:/$PLUGIN_PATH -install elasticsearch-term-plugin

3. Updating the plugin

        cd $ELASTICSEARCH_HOME
        ./bin/plugin -remove elasticsearch-term-plugin
        ./bin/plugin -url file:/$PLUGIN_PATH -install elasticsearch-term-plugin


Usage
==========

##### Version

ElasticSearch version 0.90.0

##### Facet Parameters
* fields - list of fields to examine for terms, this is the only required field
* search - substring to search for (case insensitive)
* max_per_shard - max number of terms to pull from a shard
* prefix - (true/false) defaults to false, is the search to be used for prefix/starts with matching only?
* sort -   (true/false) default to true, should returned list by alpha sorted
* case_insenstive - (true/false) defaults to true, should matching be done disregarding case

##### Facet example

	    "facets" : {
		    "term_list_facet" : {
		      "term_list" : {
		        "fields" : [ "company", "other.company" ],			
		        "search" : "alt",
		        "prefix" : false,
		        "sort"   : true,
		        "case_insenstive"   : true,
		        "max_per_shard" : 10
		      }
	    	}
	    }

###### setup test index
Note the analyzer configuration of tokenizer keyword, if you for example use the whitespace tokenizer your results will be strange

	curl -XDELETE localhost:9200/my_test_index 
	
	curl -XPOST localhost:9200/my_test_index -d '{
	    "settings" : {
	        "analysis": {
	          "analyzer": {
	            "default": {
	              "filter": "lowercase", 
	              "type": "custom", 
	              "tokenizer": "keyword"
	            	}
	        	}
	    	}
		}
	 }' 
	 
###### place test data in index

	curl -XPUT localhost:9200/my_test_index/my_test_type/1 -d '{
		"my_test_type" : {
		"company" : "Walt Disney",
		"desc" : "A movie company and a TV channel",
		"other" : {"company" : "Walt Disney Theme Parks"}
		}
	}'
	
	curl -XPUT localhost:9200/my_test_index/my_test_type/2 -d '{
		"my_test_type" : {
		"company" : "Walts Plumbing",
		"desc" : "Best plumber in town",
		"other" : {"company" : "Walts waterproof rubber chicken company"}
		}
	}'
    
###### query data with custom facet

	curl -XGET localhost:9200/my_test_index/_search?pretty=1 -d '{
	    "query" : {
	        "match_all" : {  }
	    },
	    "facets" : {
		    "term_list_facet" : {
		      "term_list" : {
		        "fields" : [ "company", "other.company" ],			
		        "search" : "alt",
		        "prefix" : false,
		        "max_per_shard" : 10
		      }
	    	}
	    }
	}'

License Notice
--------------

elasticsearch-river-kafka  
Licensed under Apache License, Version 2.0  

Copyright 2009-2013 Shay Banon and [ElasticSearch](http://http://www.elasticsearch.org/)  
Copyright 2013 [Endgame LLC](http://www.endgamesystems.com)  

This product is a software plugin developed for [ElasticSearch](http://http://www.elasticsearch.org/)  

Inspiration was taken from Andrew Clegg and his ElasticSearch Approx Plugin  
https://github.com/ptdavteam/elasticsearch-approx-plugin  


	
