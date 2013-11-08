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

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

/**
 * TermListFacetTest
 * 
 *
 */
public class TermListFacetTest extends TestCase {
	private Node node;
	private final String index = "test_index_lower";
	private final String index_mixed = "test_index_mixed";
	private final String facetName = "term_list_facet";
	private static final AtomicInteger counter = new AtomicInteger(0);
	private final boolean uselocalhost = false;		// helper method for client() will connect you to localhost if this is true
	private final Random _random = new Random(0);
	private final int randomElementsMultiplier = 100;		//use this to help control how many random elements to create
	private final int numOfElements = randomElementsMultiplier + _random.nextInt(100);
	private final int numberOfShards = 1;					
	private final int numberOfReplicas = 0;
	private List<String> testFields_name = new ArrayList<String>();
	private List<String> testFields_childName = new ArrayList<String>();
	private List<String> testFields_nameAndChildName = new ArrayList<String>();
	private List<String> testFields_childrenName = new ArrayList<String>();
	private List<String> parentRandomStrings = new ArrayList<String>();
	private List<String> childRandomStrings = new ArrayList<String>();
	private List<String> parentAndChildRandomStrings = new ArrayList<String>();
	private Set<String> uniqParentText = new HashSet<String>();
	private Set<String> uniqChildText = new HashSet<String>();
	private Set<String> uniqAllText = new HashSet<String>();
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		deleteAllIndices();
		
		node.close();
		if(!node.isClosed()){
			System.out.println("node close did not work : ");
			throw new Exception("node close did not work : ");
		}
	}
	
	@Override
	protected void setUp() throws Exception {
			super.setUp();
			
			testFields_name.add("name");
			testFields_nameAndChildName.add("name");
			testFields_nameAndChildName.add("child.name");
			testFields_childName.add("child.name");
			testFields_childrenName.add("children.name");
			
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("node.http.enabled", false)
					.put("index.gateway.type", "none")
					.put("index.number_of_shards", numberOfShards)
					.put("index.number_of_replicas", numberOfReplicas)
					.put("path.data", "target")
					.put("refresh_interval", -1)
					.put("index.cache.field.type", "soft").build();
	
			node = nodeBuilder().local(true).settings(settings).clusterName("TermListFacetTest").node();
			
			node.start();
			
			// set up rules for analysis, we want lowercase, keyword tokenization
			// (do not break up spaces)
			//
			XContentBuilder analysis = XContentFactory.jsonBuilder().startObject()
					.startObject("analysis")
					.startObject("analyzer")
					.startObject("default")
					.field("type", "custom")
					.field("tokenizer", "keyword")
					.field("filter", new String[] { "lowercase" }).endObject().endObject().endObject().endObject();
	
			XContentBuilder analysis_mixedcase = XContentFactory.jsonBuilder().startObject()
					.startObject("analysis")
					.startObject("analyzer")
					.startObject("default")
					.field("type", "custom")
					.field("tokenizer", "keyword")
					.field("filter", new String[] { "standard" })
					.endObject()
					.endObject()
					.endObject()
					.endObject();
			
			// wait for green after start
			//
			client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
			
			// this create/delete error checking is a little paranoid, but I experienced some strange node not re-creating index still exists
			// while constructing tests and running them inside ide so this is here just to be really sure indexes are created correctly for each test
			//
			try{
				CreateIndexResponse cir = client().admin().indices().prepareCreate(index).setSettings(analysis.string()).execute().actionGet();
				CreateIndexResponse cir_2 = client().admin().indices().prepareCreate(index_mixed).setSettings(analysis_mixedcase.string()).execute().actionGet();
				
				if(!cir.isAcknowledged()){
					System.out.println("Create was not acknowledged : " + index_mixed);
					throw new Exception("Failed Create of Index : " + index_mixed);
				}
				
				if(!cir_2.isAcknowledged()){
					System.out.println("Create was not acknowledged : " + index_mixed);
					throw new Exception("Failed Create of Index : " + index_mixed);
				}
				
				// wait for green after index create
				//
				client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
				
			} catch (Exception e) {		// be sure to tear down if we fail during setup, to prevent index already created issues from repeating
				deleteAllIndices();
				System.out.println("Error during Setup : " + e.toString());			
				throw new Exception("Error during Setup : ", e);
			} 
		

	}
	
	/*************************************************************************************************/
	
	/**
	 * testNonStringTermsShouldThrowError
	 * 
	 * @throws Exception
	 */
	public void testNonStringTermsShouldThrowError() throws Exception {
		runStandardPutsAndFlush(index);						//include numeric data
		List<String> randomNumField = new ArrayList<String>();
		randomNumField.add("rnum");
		boolean thrown = false;
		
		try {
			TermListFacetBuilder custom_facet = new TermListFacetBuilder(facetName).fields(randomNumField);
			
			SearchRequestBuilder srb = client().prepareSearch(index);
			srb.setSearchType(SearchType.COUNT);
			srb.addFacet(custom_facet);

			System.out.println("SearchResponse Facet : \n  " + srb.toString() + "\n");

			ListenableActionFuture<SearchResponse> laf = srb.execute();
			SearchResponse sr = laf.actionGet();
			
			fail("Never should have gotten here: ");
		} catch (Exception e) {
			e.printStackTrace(System.out);
			thrown = true;
		}
		
		assertTrue(thrown);
	}
	
	/****
	 * testMissingRequiredSearchTermsThrowsError
	 * 
	 * Test the required params "field"
	 */
	public void testMissingRequiredSearchTermsThrowsError() throws Exception {
		//build facet without fields param
		//
		TermListFacetBuilder custom_facet = new TermListFacetBuilder(facetName);
		boolean thrown = false;
		
		try {
			SearchRequestBuilder srb = client().prepareSearch(index);
			srb.setSearchType(SearchType.COUNT);
			srb.addFacet(custom_facet);
			ListenableActionFuture<SearchResponse> laf = srb.execute();
			fail("Never should have gotten here: ");
			
		} catch (Exception e) {
			thrown = true;
		}
		
		assertTrue(thrown);
	}
	
	/**
	 * testWithOnlyRequiredParams
	 * 
	 * @throws Exception
	 */
	public void testWithOnlyRequiredParams() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name);

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());

		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();

		for (Object item : entries) {
			assertTrue( parentRandomStrings.contains(item.toString()));
		}
	}
	
	/**
	 * testShardLimit
	 * 
	 * @throws Exception
	 */
	public void testShardLimit() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, "a", 1, false, true, true);

		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();
		
		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		// did the number of hits from es match the number of shards (since we asked for only 1 from each shard)
		//
		assertEquals(this.numberOfShards, entries.size());
	
	}
	
	/****
	 * Test the optional search parameters, prefix and case insensitive
	 */
	
	/**
	 * testSearchWithPrefix
	 * 
	 * @throws Exception
	 */
	public void testSearchWithPrefix() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, "a", 10000, true, true, true);

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkPrefixSearchResults(response, parentRandomStrings, "a");
	}

	/**
	 * testSearch
	 * 
	 * @throws Exception
	 */
	public void testSearch() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, "z", 10000, false, true, true);

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkContainsSearchResults(response, parentRandomStrings, "z");
	}

	/**
	 * testSearchWithCaseSensitive
	 * 
	 * @throws Exception
	 */
	public void testSearchWithCaseSensitive() throws Exception {
		runStandardPutsAndFlush(index_mixed);
		SearchResponse response = this.getTermList(index_mixed, testFields_name, "C", 10000, false, false, true);

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index_mixed));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkContainsSearchResults(response, parentRandomStrings, "C");
	}

	/**
	 * testPrefixWithCaseSensitive
	 * 
	 * @throws Exception
	 */
	public void testPrefixWithCaseSensitive() throws Exception {
		runStandardPutsAndFlush(index_mixed);
		SearchResponse response = this.getTermList(index_mixed, testFields_name, "C", 10000, true, false, true);

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index_mixed));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkPrefixSearchResults(response, parentRandomStrings, "C");
	}
	
	/**********
	 * Test for parent level field, sub object/child field, and both
	 */

	/**
	 * testParentLevelFieldOfNestedObjects
	 * 
	 * @throws Exception
	 */
	public void testParentLevelFieldOfNestedObjects() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, null, 10000, false, true, true); 

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());

		checkStringSearchResponse(response, numOfElements, uniqParentText.size(), parentRandomStrings);
	}

	/**
	 * testChildLevelFieldOfNestedObjects
	 * 
	 * @throws Exception
	 */
	public void testChildLevelFieldOfNestedObjects() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_childName, null, 10000, false, true, true); 

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkStringSearchResponse(response, numOfElements, uniqChildText.size(), childRandomStrings);
	}

	/**
	 * testArrayOfChildrenFieldOfNestedObjects
	 * can we locate terms that are nested inside an array
	 * 
	 * @throws Exception
	 */
	public void testArrayOfChildrenFieldOfNestedObjects() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_childrenName, "a", 10000, false, true, true); 
		
		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkContainsSearchResults(response, childRandomStrings, "a");
	}
	
	/**
	 * testChildAndParentLevelFieldOfNestedObjects
	 * 
	 * @throws Exception
	 */
	public void testChildAndParentLevelFieldOfNestedObjects() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_nameAndChildName, null, 10000, false, true, true); 

		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		checkStringSearchResponse(response, numOfElements, uniqAllText.size(), parentAndChildRandomStrings);
	}

	/**
	 * testNoSort
	 * 
	 * @throws Exception
	 */
	public void testNoSort() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, null, 10000, false, true, false); 
		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();
		
		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		// are we unsorted
		List copy = new ArrayList(entries);
	    Collections.sort(copy);
	    assertEquals(false, copy.equals(entries));
	}
	
	
	
	/**
	 * testSort
	 * 
	 * @throws Exception
	 */
	public void testSort() throws Exception {
		runStandardPutsAndFlush(index);
		SearchResponse response = this.getTermList(index, testFields_name, null, 10000, false, true, true); 
		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();
		
		// did the number of hits from es match the number we put in?
		//
		assertEquals(numOfElements, countAll(index));
		assertEquals(numOfElements, response.getHits().getTotalHits());
		
		// are we sorted
		List copy = new ArrayList(entries);
	    Collections.sort(copy);
	    assertEquals(copy, entries);
	}
	
	/**
	 * checkContainsSearchResults
	 * 
	 * @param response
	 * @param putStringList
	 * @param searchTerm
	 */
	private void checkContainsSearchResults(SearchResponse response, List<String> putStringList, String searchTerm)
	{
		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();
		
		// is each member of the facet list a member of the list we sent in?
		// does each member of the facet list contain our search term
		for (Object item : entries) {
			assertTrue( putStringList.contains(item.toString()));
			assertTrue( item.toString().contains(searchTerm));
		}
	}
	
	/**
	 * checkPrefixSearchResults
	 * 
	 * @param response
	 * @param putStringList
	 * @param searchTerm
	 */
	private void checkPrefixSearchResults(SearchResponse response, List<String> putStringList, String searchTerm)
	{
		List<? extends Object> entries = ((TermListFacet) response.getFacets().facet(facetName)).entries();
		
		// is each member of the facet list a member of the list we sent in?
		// does each member of the facet list start with the search term
		for (Object item : entries) {
			assertTrue( putStringList.contains(item.toString()));
			assertTrue( item.toString().startsWith(searchTerm));
		}
	}
	
	/**
	 * checkStringSearchResponse 
	 * 
	 * Does the response contain the correct number of
	 * unique items? Are the values in the response ones that we placed in ES?
	 * 
	 * @param sr
	 * @param numOfDocs
	 * @param numOfElements
	 * @param words
	 * 
	 */
	private void checkStringSearchResponse(final SearchResponse sr, final int numOfElements, final int numOfUniqueElements, final List<String> allPossibleWords) {
		TermListFacet facet = sr.getFacets().facet(facetName);
		final List<? extends Object> entries = facet.entries();
		final int len = entries.size();

		// is our results list as long as the unique list we sent in?
		//
		assertEquals(len, numOfUniqueElements);

		// is each member of the facet list a member of the list we sent in?
		//
		for (final Object item : entries)
			assertTrue( allPossibleWords.contains(item.toString()));

	}
	
	/*****************************************************************************************************/
	
	/**
	 * deleteAllIndices
	 * 
	 * @throws Exception
	 */
	private void deleteAllIndices() throws Exception
	{
		IndicesExistsResponse af = client().admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
		if(af.isExists()) {
			DeleteIndexResponse dir = client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
			
			if(!dir.isAcknowledged()){
				System.out.println("Delete was not acknowledged" + index);
				throw new Exception("Failed Delete of Index : " + index);
			}
		}
		
		IndicesExistsResponse af_2 = client().admin().indices().exists(new IndicesExistsRequest(index_mixed)).actionGet();
		if(af_2.isExists()) {
			DeleteIndexResponse dir_2 = client().admin().indices().delete(new DeleteIndexRequest(index_mixed)).actionGet();
			
			if(!dir_2.isAcknowledged()){
				System.out.println("Delete was not acknowledged : " + index_mixed);
				throw new Exception("Failed Delete of Index : " + index_mixed);
			}
		}
		
		// wait for green after delete
		//
		client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
	}
	
	/**
	 * runStandardPutsAndFlush
	 * generate random data, count it for comparison later
	 * call es with puts of the generated data
	 * 
	 * @param index
	 * @throws IOException
	 */
	/*public void runStandardPutsAndFlush(String index) throws IOException
	{
		this.runStandardPutsAndFlush(index, true);
	}*/
	
	public void runStandardPutsAndFlush(String index) throws IOException
	{
		parentRandomStrings = new ArrayList<String>();
		childRandomStrings = new ArrayList<String>();
		parentAndChildRandomStrings = new ArrayList<String>();
		
		parentRandomStrings = this.generateRandomStrings(numOfElements);
		childRandomStrings = this.generateRandomStrings(numOfElements);
	
		List<Map<String, Object>> listofmaps = this.generateSimpleNestedData(parentRandomStrings, childRandomStrings, numOfElements);

		for (int i = 0; i < numOfElements; i++) {
			this.putSimpleNestedData(index, "test_type", listofmaps.get(i));
		}

		for (String s : parentRandomStrings) {
			uniqParentText.add(s);
		}
		
		for (String s : childRandomStrings) {
			uniqChildText.add(s);
		}
		
		uniqAllText.addAll(uniqParentText);
		uniqAllText.addAll(uniqChildText);
		
		parentAndChildRandomStrings.addAll(parentRandomStrings);
		parentAndChildRandomStrings.addAll(childRandomStrings);
		
		flush(index);
	}
	
	/**
	 * getTermList
	 * 
	 * @param index
	 * @param keyFields
	 * @param searchText
	 * @param maxPerShard
	 * @param prefix
	 * @return
	 */
	private SearchResponse getTermList(String index, List<String> fields, String searchText, int maxPerShard, boolean prefix, boolean caseInsensitive, boolean sort) {

		TermListFacetBuilder custom_facet = new TermListFacetBuilder(facetName).fields(fields).maxPerShard(maxPerShard).prefix(prefix).sort(sort).caseInsensitive(caseInsensitive).search(searchText);
		SearchResponse custom_sr = null;

		try {
			SearchRequestBuilder srb = client().prepareSearch(index);
			srb.setSearchType(SearchType.COUNT);
			srb.addFacet(custom_facet);

			System.out.println("SearchRequestBuilder Facet : \n  " + srb.toString() + "\n");

			ListenableActionFuture<SearchResponse> laf = srb.execute();
			custom_sr = laf.actionGet();

		} catch (Exception e) {
			e.printStackTrace(System.out);
			fail("this test failed");
		}

		System.out.println("SearchResponse : \n " + custom_sr.toString());
		return custom_sr;
	}

	/**
	 * getTermList
	 * 
	 * @param index
	 * @param fields
	 * @return
	 */
	private SearchResponse getTermList(String index, List<String> fields) {
		TermListFacetBuilder custom_facet = new TermListFacetBuilder(facetName).fields(fields);
		SearchResponse custom_sr = null;

		try {
			SearchRequestBuilder srb = client().prepareSearch(index);
			srb.setSearchType(SearchType.COUNT);
			srb.addFacet(custom_facet);

			System.out.println("SearchResponse Facet : \n  " + srb.toString() + "\n");

			ListenableActionFuture<SearchResponse> laf = srb.execute();
			custom_sr = laf.actionGet();

		} catch (Exception e) {
			System.out.println("Exception e : " + e);
			StackTraceElement[] elements = e.getStackTrace();

			for (int i = 0; i < elements.length; i++) {
				System.out.println(elements[i]);
			}
			
			fail("this test failed");
		}

		System.out.println("SearchResponse : \n " + custom_sr.toString());
		return custom_sr;
	}
	
	/**
	 * generateRandomStrings
	 * 
	 * @param numberOfWords
	 * @return
	 */
	private List<String> generateRandomStrings(int numberOfWords) {
		return this.generateRandomStrings(numberOfWords, true);
	}
		
	/**
	 * generateRandomStrings
	 * 
	 * @param numberOfWords
	 * @param lowercaseonly
	 * @return
	 */
	private List<String> generateRandomStrings(final int numberOfWords, boolean lowercaseonly) {
		final String[] randomStrings = new String[numberOfWords];
		List<String> myList = new ArrayList<>();
		for (int i = 0; i < numberOfWords; i++) {
			final char[] word = new char[_random.nextInt(8) + 3];

			for (int j = 0; j < word.length; j++) {
				if (lowercaseonly) {
					word[j] = (char) ('a' + _random.nextInt(26));
				} else {
					if (_random.nextInt(10) > 5) {
						word[j] = (char) ('A' + _random.nextInt(26));
					} else {
						word[j] = (char) ('a' + _random.nextInt(26));
					}
				}
			}

			if (_random.nextInt(10) > 5) {
				final char[] word2 = new char[_random.nextInt(8) + 3];

				for (int k = 0; k < word2.length; k++) {
					word2[k] = (char) ('a' + _random.nextInt(26));
				}

				randomStrings[i] = new String(word) + " " + new String(word2);
			} else {
				randomStrings[i] = new String(word);
			}

			myList.add(randomStrings[i]);
		}

		return myList;
	}

	/**
	 * putSimpleNestedData
	 * 
	 * @param index
	 * @param type
	 * @param data
	 * @throws IOException
	 */
	private void putSimpleNestedData(String index, String type, Map<String, Object> data) throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("myid", data.get("id"))
				.field("name", data.get("name"))
				.field("rnum", data.get("rnum"))
				.field("child", data.get("child"))				
				.field("children", data.get("children"))		
				.endObject();
		
		//put the data in es
		client().prepareIndex(index, type, (String) data.get("id")).setRefresh(true).setRouting((String) data.get("id")).setSource(builder).execute().actionGet();
		client().prepareGet(index, type, (String) data.get("id")).execute().actionGet();
	}

	/**
	 * @param index
	 */
	private void flush(String index) 
	{
		// flush it to ensure data is present
		client().admin().indices().flush(new FlushRequest(index)).actionGet();
		client().admin().indices().refresh(new RefreshRequest()).actionGet();
	}
	
	/**
	 * generateSimpleNestedData
	 * 
	 * @param count
	 * @return
	 * @throws IOException
	 */
	private List<Map<String, Object>> generateSimpleNestedData(List<String> randomParentNames, List<String> randomChildNames, int count) throws IOException {
		List<Map<String, Object>> simpleNestedList = new ArrayList<Map<String, Object>>();

		for (int i = 0; i < randomParentNames.size(); i++) {
			String stringID = String.valueOf(newID());
			Map<String, Object> parentData = new HashMap<String, Object>();
			parentData.put("id", stringID);
			parentData.put("name", randomParentNames.get(i));
			parentData.put("rnum", (Integer) _random.nextInt());
			
			Map<String, Object> subData = new HashMap<String, Object>();
			subData.put("name", randomChildNames.get(i));

			Map<String, Object> subData2 = new HashMap<String, Object>();
			subData.put("name", randomChildNames.get(i));

			List<Map<String, Object>> children = new ArrayList<Map<String, Object>>();
			children.add(subData);
			children.add(subData2);
			
			parentData.put("child", subData);
			parentData.put("children", children.toArray());
			
			simpleNestedList.add(parentData);
		}

		return simpleNestedList;
	}

	private static int newID() {
		return counter.getAndIncrement();
	}
	
	private long countAll(String index) {
		return client().prepareCount(index).execute().actionGet().getCount();
	}

	private Client client() {
		if (uselocalhost) {
			return localclient();
		} else {
			return node.client();
		}
	}

	/**
	 * helper if you want to hit local es install to do some testing
	 * 
	 * @return
	 */
	private Client localclient() {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		return new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
	}

}
