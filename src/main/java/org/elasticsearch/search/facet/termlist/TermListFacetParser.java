/* Copyright 2013 Endgame, Inc.
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
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetExecutor.Mode;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

public class TermListFacetParser extends AbstractComponent implements FacetParser {

	/**
	 * /** The type of the facet, for example, terms.
	 * 
	 * String[] types();
	 * 
	 * 
	 * 
	 * /** The default mode to use when executed as a "main" (query level)
	 * facet.
	 * 
	 * FacetExecutor.Mode defaultMainMode();
	 * 
	 * /** The default mode to use when executed as a "global" (all docs) facet.
	 * 
	 * FacetExecutor.Mode defaultGlobalMode();
	 * 
	 * /** Parses the facet into a {@link FacetExecutor}.
	 * 
	 * FacetExecutor parse(String facetName, XContentParser parser,
	 * SearchContext context) throws IOException;
	 */

	@Override
	public String[] types() {
		return new String[] { TermListFacet.TYPE, "term_list_facet" };
	}

	/**
	 * Instantiates a new term list facet processor.
	 * 
	 * @param settings
	 *            the settings
	 */
	@Inject
	public TermListFacetParser(final Settings settings) {
		super(settings);
		InternalTermListFacet.registerStreams();
	}

	public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
		List<String> fields = null;
		XContentParser.Token token;
		String currentfieldName = null;
		String searchText = null;
		boolean prefix = false;
		boolean caseInsenstive = true;
		boolean sort = true;
		int maxPerShard = 100;

		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

			if (token == XContentParser.Token.FIELD_NAME) {
				currentfieldName = parser.currentName();
			} else if (token.isValue()) {
				if ("max_per_shard".equals(currentfieldName)) {
					maxPerShard = parser.intValue();
				} else if ("search".equals(currentfieldName)) {
					searchText = parser.text();
				} else if ("prefix".equals(currentfieldName)) {
					prefix = parser.booleanValue();
				} else if ("case_insenstive".equals(currentfieldName)) {
					caseInsenstive = parser.booleanValue();
				} else if ("sort".equals(currentfieldName)) {
					sort = parser.booleanValue();
				}
			} else if (token == XContentParser.Token.START_ARRAY) {
				if ("fields".equals(currentfieldName)) {
					fields = new ArrayList<String>();
					while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
						fields.add(parser.text());
					}
				}
			}
		}

		//a field is required
		if (fields == null || fields.isEmpty()) {
			throw new FacetPhaseExecutionException(facetName, "fields is required to be set for term list facet, either using [fields]");
		}

		// check fields for correct mapping
		for (String field : fields) {
			final FieldMapper mapper = context.smartNameFieldMapper(field);
			if (mapper == null) {
				logger.warn("No mapping found for Field : {} ", field);
				throw new FacetPhaseExecutionException(facetName, "(key) field [" + field + "] not found");

			}

			if (!"string".equals(mapper.fieldDataType().getType())) {
				logger.warn("No String mapping found for Field : {} ", field);
				throw new FacetPhaseExecutionException(facetName, "No String mapping found for field [" + field + "] not found");
			}
		}

		return new TermListFacetExecutor(facetName, fields, searchText, prefix, context, maxPerShard, caseInsenstive, sort);
	}

	@Override
	public Mode defaultMainMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

	@Override
	public Mode defaultGlobalMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

}
