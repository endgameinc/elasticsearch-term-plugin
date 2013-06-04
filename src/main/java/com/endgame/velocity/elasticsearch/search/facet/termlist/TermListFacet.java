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
package com.endgame.velocity.elasticsearch.search.facet.termlist;

import java.util.List;

import org.elasticsearch.search.facet.Facet;

/**
 * Defines the content and abilities of a facet class.
 *
 */
public interface TermListFacet extends Facet, Iterable<Object> {
	
    /**
     * The type of the facet.
     */
    public static final String TYPE = "term_list";

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> entries();

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> getEntries();

}
