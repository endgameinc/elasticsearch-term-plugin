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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.ComparatorType;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet.TermEntry;

/**
 * InternalTermListFacet
 * Only handle Strings
 * 
 */
public class InternalTermListFacet extends InternalFacet implements TermListFacet {

	private final String type = "term_list";
	private static final BytesReference STREAM_TYPE = new HashedBytesArray(TermListFacet.TYPE.getBytes());
	private Object[] strings; 			
	private boolean sort;		

    /**
     * Instantiates a new internal string term list facet.
     *
     * @param facetName the facet name
     * @param strings the strings
     */
    public InternalTermListFacet(final String facetName, final Object[] strings, boolean sort) {
    	super(facetName);
        this.strings = strings;
        this.sort = sort;
    }
    
	/**
	 * Instantiates a new internal term list facet.
	 */
	private InternalTermListFacet() {
	}

	/**
	 * Register streams.
	 */
	public static void registerStream() {
		Streams.registerStream(STREAM, STREAM_TYPE);
	}

	/** The stream. */
	static Stream STREAM = new Stream() {
		@Override
		public Facet readFacet(StreamInput in) throws IOException {
			return readTermListFacet(in);
		}
	};

	/**
	 * Read term list facet.
	 * 
	 * @param in
	 *            the input stream
	 * @return the internal term list facet
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static InternalTermListFacet readTermListFacet(final StreamInput in) throws IOException {
		final InternalTermListFacet facet = new InternalTermListFacet();
		facet.readFrom(in);		
		return facet;
	}

	@Override
    public void writeTo(final StreamOutput out) throws IOException {
		int type = 0;
		int size = strings.length;
		super.writeTo(out);
		out.writeVInt(size);
		for (int i = 0; i < size; i++) {
			out.writeString((String) strings[i]);
		}
    }
	
	@Override
	public void readFrom(final StreamInput in) throws IOException {
		super.readFrom(in);
		final int size = in.readVInt();
		strings = new Object[size];
		for (int i = 0; i < size; i++) {
			strings[i] = in.readString();
		}
	}

    /**
     * Output JSON fields
     */
    static final class Fields {
        /** The Constant _TYPE. */
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");

        /** The Constant ENTRIES. */
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
    }
	
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(this.getName());
        builder.field(Fields._TYPE, TermListFacet.TYPE);
        builder.array(Fields.ENTRIES, strings);
        builder.endObject();
        return builder;
    }

	@Override
	public String getType() {
		return type;
	}
	
	@Override
	public BytesReference streamType() {
		return STREAM_TYPE;
	}

    @Override
    public Facet reduce(ReduceContext context) {
        return myReduce(this.getName(), context.facets());
    }
	
    /**
     * Takes a list of facets and returns a new facet containing the merged data from all of them.
     *
     * @param name the facet name
     * @param facets the facets
     * @return the resulting reduced facet
     */
    public Facet myReduce(final String name, final List<Facet> facets) {
        final Set<String> reducedStrings = new HashSet<String>();

        for(final Facet facet : facets) {
            final InternalTermListFacet itlf = (InternalTermListFacet) facet;
            for(final Object obj : itlf.strings) {
                reducedStrings.add(obj.toString());
            }
        }

    	 String[] strArr = reducedStrings.toArray( new String[ reducedStrings.size() ] );
    	 
    	 if(sort)
    		 Arrays.sort( strArr );
         
    	 return new InternalTermListFacet(name,  strArr, sort );
    }
	
	@Override
	public Iterator<Object> iterator() {
		 return entries().iterator();
	}

	@Override
	public List<Object> entries() {
         return Arrays.asList(strings);
	}

	@Override
	public List<? extends Object> getEntries() {
		return entries();
	}
}
