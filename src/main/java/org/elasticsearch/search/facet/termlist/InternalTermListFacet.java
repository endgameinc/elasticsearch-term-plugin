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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.google.common.collect.Lists;



/**
 * InternalTermListFacet
 * Only handle Strings
 * 
 */
public class InternalTermListFacet extends InternalFacet implements TermListFacet {

	private final String type = "term_list";
	private static final BytesReference STREAM_TYPE = new HashedBytesArray(TermListFacet.TYPE.getBytes());
	private Object[] strings; 	
	private String name; 		// plugin name
	private boolean sort;		

    /**
     * Instantiates a new internal string term list facet.
     *
     * @param facetName the facet name
     * @param strings the strings
     */
    public InternalTermListFacet(final String facetName, final Object[] strings, boolean sort) {
    	super(facetName);
    	this.name = facetName;
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
	public static void registerStreams() {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.elasticsearch.common.io.stream.Streamable#readFrom(org.elasticsearch
	 * .common.io.stream.StreamInput)
	 */
	@Override
	public void readFrom(final StreamInput in) throws IOException {
		name = in.readString();
		final int size = in.readVInt();
		final byte dataType = in.readByte();
		switch (dataType) {
		case 0:
			strings = Lists.newArrayListWithCapacity(size).toArray();
			break;
		default:
			throw new IllegalArgumentException("dataType " + dataType + " is not known");
		}
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
	
    /* (non-Javadoc)
     * @see org.elasticsearch.common.xcontent.ToXContent#toXContent(org.elasticsearch.common.xcontent.XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(name);
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
		return myReduce(name, context.facets());
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
