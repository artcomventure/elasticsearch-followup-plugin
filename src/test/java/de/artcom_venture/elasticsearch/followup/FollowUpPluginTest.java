package de.artcom_venture.elasticsearch.followup;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;
import java.util.Scanner;

import junit.framework.TestCase;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

public class FollowUpPluginTest extends TestCase {

    private Node node;
    private static Random randomGenerator = new Random();
    private static final String ES_INDEX = "myindex";
    private static final String ES_TYPE = "mytype";
    private static final String PLUGIN_NAME = "followup";
    private static final String HOST = "127.0.0.1";
    private static final String PLUGIN_URL_LIST = "http://" + HOST + ":9200/" + ES_INDEX + "/_" + PLUGIN_NAME;

    @Override
    public void setUp() throws Exception {
    	ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder();
	    settings.put("node.name", "test");
	    settings.put("index.number_of_shard", "1");
	    settings.put("index.number_of_replicas", "0");
	    settings.put("discovery.zen.ping.multicast.enabled", "false");
	    settings.put("path.data", "target/data");
	    settings.put("network.publish_host", HOST);
	    this.node = NodeBuilder.nodeBuilder().settings(settings).clusterName("test").data(true).local(true).node();
	    
	    // create index
    	node.client().admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
	    node.client().admin().indices().create(Requests.createIndexRequest(ES_INDEX)).actionGet();
	    
	    // create type
	    XContentBuilder mapping = XContentFactory.jsonBuilder()
	        .startObject()
	             .startObject(ES_TYPE)
	                  .startObject("properties")
	                      .startObject("key")
	                          .field("type", "string")
	                          .field("index", "not_analyzed")
	                       .endObject()
	                       .startObject("value")
	                          .field("type","long")
	                       .endObject()
	                  .endObject()
	              .endObject()
	           .endObject();
	    node.client().admin().indices().preparePutMapping(ES_INDEX).setType(ES_TYPE).setSource(mapping).execute().actionGet();
	    
	    // create initial document
	    indexDocument("1");
    }

    @Override
    public void tearDown() throws Exception {
    	this.node.close();
    }
    
    private int getChangesLength() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getJSONArray(ES_INDEX).length();
    }
    
    private int stopListener() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?stop").openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getInt("status");
    }
    
    private int startListener() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?start").openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getInt("status");
    }
    
    private void indexDocument(String id) throws IOException {
    	XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
    		.startObject()
    			.field("key", "Lorem ipsum")
    			.field("value", randomGenerator.nextLong())
    		.endObject();
		node.client().index(new IndexRequest(ES_INDEX, ES_TYPE).source(sourceBuilder).id(id)).actionGet();
    }
    
    private void deleteDocument(String id) throws IOException {
    	node.client().prepareDelete(ES_INDEX, ES_TYPE, id).execute().actionGet();
    }
    
    private void createDocument() throws IOException {
    	XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
    		.startObject()
    			.field("key", "Lorem ipsum")
    			.field("value", randomGenerator.nextLong())
    		.endObject();
		node.client().index(new IndexRequest(ES_INDEX, ES_TYPE).source(sourceBuilder)).actionGet();
    }
    
    public void testCompatibility() throws Exception {
    	String pluginVersion = "-";
    	NodesInfoResponse nodesInfoResponse = node.client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getInfos()) {
        	if (pluginInfo.getName().equals(PLUGIN_NAME)) {
        		pluginVersion = pluginInfo.getVersion();
        	}
        }
        assertEquals(pluginVersion, node.client().admin().cluster().prepareNodesInfo().get().getNodes()[0].getVersion().toString());
    }
    
    public void testStart() throws MalformedURLException, IOException {
    	assertEquals(200, startListener());
    }

    public void testStop() throws MalformedURLException, IOException {
    	assertEquals(200, stopListener());
    }
    
    public void testListener() throws Exception {
		assertEquals(0, getChangesLength());
		
		// not listening
		indexDocument("2");
		assertEquals(0, getChangesLength());
		
		// listening
		assertEquals(200, startListener());
		indexDocument("3");
		assertEquals(1, getChangesLength());

		// start resets the list
		assertEquals(200, startListener());
		assertEquals(0, getChangesLength());
		indexDocument("4");
		indexDocument("5");
		assertEquals(2, getChangesLength());
		
		// not listening
		assertEquals(200, stopListener());
		indexDocument("6");
		assertEquals(2, getChangesLength());
    }
    
    public void testBuffer() throws Exception {
		startListener();
		assertEquals(0, getChangesLength());
		createDocument();
		indexDocument("a");
		deleteDocument("a");
		indexDocument("b");
		stopListener();
		assertEquals(4, getChangesLength());
		
		// latest changes stored
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		JSONArray changes = response.getJSONArray(ES_INDEX);
		
		assertEquals("CREATE", changes.getJSONObject(0).getString("operation"));
		
		assertEquals("a", changes.getJSONObject(1).getString("id"));
		assertEquals("INDEX", changes.getJSONObject(1).getString("operation"));
		
		assertEquals("a", changes.getJSONObject(2).getString("id"));
		assertEquals("DELETE", changes.getJSONObject(2).getString("operation"));
		
		assertEquals("b", changes.getJSONObject(3).getString("id"));
		assertEquals("INDEX", changes.getJSONObject(3).getString("operation"));
    }
}
