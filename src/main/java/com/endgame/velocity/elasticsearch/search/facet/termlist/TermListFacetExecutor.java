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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Defines the content and abilities of a facet class.
 */
public class TermListFacetExecutor extends FacetExecutor {
	protected final ESLogger logger = Loggers.getLogger(getClass());
	private List<String> fields;
	private String search;
	private boolean prefix;
	private Collection<String> uniqueTerms;
	private final int maxPerShard;
	private boolean caseInsensitive;
	private boolean sort;


	public TermListFacetExecutor(String facetName, List<String> fields, String search, boolean prefix, SearchContext sc, int maxPerShard, boolean caseInsensitive, boolean sort) {
		logger.debug("TermListFacetExecutor : constructor : START {} : {} : {} ", facetName , fields , search);

		this.fields = fields;
		this.maxPerShard = maxPerShard;
		this.search = search;
		this.prefix = prefix;
		this.caseInsensitive = caseInsensitive;
		this.uniqueTerms = new HashSet<String>();
		this.sort = sort;
		
		logger.debug("TermListFacetExecutor : constructor : END ");
	}

	@Override
	public InternalFacet buildFacet(String facetName) {
		logger.debug("TermListFacetExecutor : buildFacet : CALLED {} : {} " ,  facetName , uniqueTerms);

		return new InternalTermListFacet(facetName, uniqueTerms.toArray(), sort);
	}

	@Override
	public Collector collector() {
		logger.debug("TermListFacetExecutor : collector : CALLED ");

		return new MyCollector(fields, search, caseInsensitive);
	}

	public class MyCollector extends FacetExecutor.Collector {
		private List<String> fields;
		private String search;
		private boolean caseInsensitive;
		
		public MyCollector(List<String> fields, String search, boolean caseInsensitive) {
			this.fields = fields;
			this.search = search;
			this.caseInsensitive = caseInsensitive;
		}

		@Override
		public void postCollection() {
			logger.debug("MyCollector : postCollection : CALLED");

			// do nothing
		}

		@Override
		public void collect(int doc) throws IOException {
			logger.debug("MyCollector : collect : CALLED ");

			// do nothing
		}

		@Override
		public void setNextReader(AtomicReaderContext context) throws IOException {
			logger.debug("MyCollector : setNextReader : START : {} : {} " , search , fields );

			if(logger.isDebugEnabled())
			{
				Fields printDebugfields = context.reader().fields();
				StringBuilder sb = new StringBuilder();
				for (String field : printDebugfields) {
					sb.append(field);
					sb.append(", ");
				}
				logger.debug("Fields in this Context : [ {} ]", sb);
			}
			
			Terms terms = null;
			TermsEnum te = null;
			BytesRef byteRef = null;
			String formattedSearch = search;
			
			if(caseInsensitive && formattedSearch != null)
			{
				formattedSearch = search.toLowerCase();
			} 
			
			// two optional flags can control the matching done here
			// prefix - true/false to decide to match only the start of the terms
			// caseInsensitive - true/false do we care about the case of the search string and the term
			// 
			
			outerloopoffields:
			for (String myfield : this.fields) {
				terms = context.reader().terms(myfield);
				
				if (terms != null) {
					te = terms.iterator(null);
					byteRef = null;
					
					while ((byteRef = te.next()) != null) {
						String termStr = new String(byteRef.bytes, byteRef.offset, byteRef.length);
						String formattedTerm = termStr;
						
						if(caseInsensitive)
						{
							formattedTerm = termStr.toLowerCase();
						}
						
						if(logger.isDebugEnabled())
						{
							logger.debug("MyCollector : setNextReader : {} : {} : {} : {} : {} : {} ", 
										formattedTerm , 
										formattedSearch , 
										(formattedSearch!=null?formattedTerm.contains(formattedSearch):"null") ,  
										(formattedSearch!=null?formattedTerm.startsWith(formattedSearch):"null") , 
										prefix , 
										caseInsensitive);
						}
						
						if (uniqueTerms.size() < maxPerShard) {
							if (search != null) {
								if(prefix && formattedTerm.startsWith(formattedSearch))			
								{
									uniqueTerms.add(termStr);
								} else if (!prefix && formattedTerm.contains(formattedSearch)) {
									uniqueTerms.add(termStr);
								}
							} else {
								uniqueTerms.add(termStr);	//everything matches, no search term
							}
						} else {
							logger.debug("BREAKING LOOP shardlimit hit : {} : {} " , maxPerShard , uniqueTerms.size());
							break outerloopoffields;		//lets get out of here, we have hit our max number
						}
					}
				} else {
					logger.debug("MyCollector : setNextReader : No terms found for field : {} ", myfield);
				}
			}

			logger.debug("MyCollector : setNextReader : EXIT ");
		}
	}

}
