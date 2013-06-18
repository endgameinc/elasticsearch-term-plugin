/* Copyright 2013 Endgame Inc
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
package org.elasticsearch.search.facet.termlist;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;

/**
 * The Class TermListFacetBuilder.
 */
public class TermListFacetBuilder extends FacetBuilder {

    private List<String> fields;
    private String search;
    private int maxPerShard;
    private boolean prefix = false;
    private boolean caseInsensitive = true;
    private boolean sort = true;
    
    /**
     * Instantiates a new term list facet builder.
     *
     * @param name the facet name
     */
    protected TermListFacetBuilder(final String name) {
        super(name);
    }

    /**
     * @param prefix
     * @return
     */
    public TermListFacetBuilder prefix(final boolean prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * @param sort
     * @return
     */
    public TermListFacetBuilder sort(final boolean sort) {
        this.sort = sort;
        return this;
    }
    
    /**
     * @param caseInsensitive
     * @return
     */
    public TermListFacetBuilder caseInsensitive(final boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }
    
    /**
     * The field names to retrieve terms for the TermListFacet.
     *
     * @param keyField the key field
     * @return the term list facet builder
     */
    public TermListFacetBuilder fields(List<String> fields) {
        this.fields = fields;
        return this;
    }
    
    /**
     * The field name to subquery match for in terms list.
     *
     * @param keyField the key field
     * @return the term list facet builder
     */
    public TermListFacetBuilder search(final String search) {
        this.search = search;
        return this;
    }

    /**
     * Max term results per shard. Defaults to 1000.
     *
     * @param maxPerShard the max number of results per shard
     * @return the term list facet builder
     */
    public TermListFacetBuilder maxPerShard(final int maxPerShard) {
        this.maxPerShard = maxPerShard;
        return this;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.xcontent.ToXContent#toXContent(org.elasticsearch.common.xcontent.XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        
    	if( fields == null || fields.isEmpty() ) {
            throw new SearchSourceBuilderException("field name must be set for term list facet [" + name + "]");
        }
        
        builder.startObject(name);
        builder.startObject(TermListFacet.TYPE);
        
        if(fields != null)
        	builder.field("fields", fields);
        
        if(search != null)
        	builder.field("search", search);
        
        builder.field("prefix", prefix);
        builder.field("sort", sort);
        builder.field("case_insenstive", caseInsensitive);
        
        if(maxPerShard > 0)
            builder.field("max_per_shard", maxPerShard);
        else
            builder.field("max_per_shard", 1000);
        
        builder.endObject();
        addFilterFacetAndGlobal(builder, params);
        
        builder.endObject();
        
        return builder;
    }

}
