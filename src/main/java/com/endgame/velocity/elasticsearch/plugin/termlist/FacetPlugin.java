/* Copyright 2013 Endgame LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.endgame.velocity.elasticsearch.plugin.termlist;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

import com.endgame.velocity.elasticsearch.search.facet.termlist.InternalTermListFacet;
import com.endgame.velocity.elasticsearch.search.facet.termlist.TermListFacetParser;


/**
 * This class registers the facets themselves with ES, as well as the stream classes
 * which govern how a facet is deserialized.
 */
public class FacetPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "facet-term-list-plugin";
    }

    @Override
    public String description() {
        return "an ES plugin with similar functionality to Lucene/Solr's TermsComponent";
    }

    @Override
    public void processModule(final Module module) {
        
    	if(module instanceof FacetModule)
    	{
    		((FacetModule) module).addFacetProcessor(TermListFacetParser.class);			// our processor goes here
    		InternalTermListFacet.registerStreams();
    	}
    }
}
