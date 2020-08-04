package de.artcom_venture.elasticsearch.followup;

import junit.framework.TestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Random;
import java.util.Scanner;

public class FollowUpPluginTest extends TestCase {

    private Client client;
    private static Random randomGenerator = new Random();
    private static final String ES_INDEX = "myindex";
    private static final String ES_TYPE = "mytype";
    private static final String PLUGIN_NAME = "followup";
    private static final String HOST = "127.0.0.1";
    private static final String PLUGIN_URL_LIST = "http://" + HOST + ":9200/" + ES_INDEX + "/_" + PLUGIN_NAME;

    @Override
    public void setUp() throws Exception {
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName(HOST), 9300));

        // create index
        client.admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
        client.admin().indices().create(Requests.createIndexRequest(ES_INDEX)).actionGet();

        // create type
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(ES_TYPE)
                        .startObject("properties")
                        .endObject()
                    .endObject()
                .endObject();
        client.admin().indices().preparePutMapping(ES_INDEX).setType(ES_TYPE).setSource(mapping).execute().actionGet();

        // create initial document
        clearListener();
        indexDocument("1");
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
    }

    private int getChangesLength() throws IOException {
        JSONObject response;
        try (Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream())) {
            response = new JSONObject(scanner.useDelimiter("\\Z").next());
        }
        return response.getJSONArray(ES_INDEX).length();
    }

    private int stopListener() throws IOException {
        JSONObject response;
        try (Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?stop").openStream())) {
            response = new JSONObject(scanner.useDelimiter("\\Z").next());
        }
        return response.getInt("status");
    }

    private int startListener() throws IOException {
        JSONObject response;
        try (Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?start").openStream())) {
            response = new JSONObject(scanner.useDelimiter("\\Z").next());
        }
        return response.getInt("status");
    }

    private int clearListener() throws IOException {
        JSONObject response;
        try (Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?clear").openStream())) {
            response = new JSONObject(scanner.useDelimiter("\\Z").next());
        }
        return response.getInt("status");
    }

    private void indexDocument(String id) throws IOException {
        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("key", "Lorem ipsum")
                .field("value", randomGenerator.nextLong())
                .endObject();
        client.index(new IndexRequest(ES_INDEX).source(sourceBuilder).id(id)).actionGet();
    }

    private void deleteDocument(String id) {
        client.prepareDelete(ES_INDEX, ES_TYPE, id).execute().actionGet();
    }

    private void createDocument() throws IOException {
        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("key", "Lorem ipsum")
                .field("value", randomGenerator.nextLong())
                .endObject();
        client.index(new IndexRequest(ES_INDEX).source(sourceBuilder)).actionGet();
    }

    public void testCompatibility() {
        String pluginVersion = "-";
        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().clear().all().get();
        for (PluginInfo pluginInfo : nodesInfoResponse.getNodes().get(0).getInfo(PluginsAndModules.class).getPluginInfos()) {
            if (pluginInfo.getName().equals(PLUGIN_NAME)) {
                pluginVersion = pluginInfo.getVersion();
            }
        }
        assertTrue(pluginVersion.startsWith(client.admin().cluster().prepareNodesInfo().get().getNodes().get(0).getVersion().toString()));
    }

    public void testStart() throws IOException {
        assertEquals(200, startListener());
    }

    public void testStop() throws IOException {
        assertEquals(200, stopListener());
    }

    public void testClear() throws IOException {
        assertEquals(200, clearListener());
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

        JSONObject response;
        try (Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream())) {
            response = new JSONObject(scanner.useDelimiter("\\Z").next());
            JSONArray changes = response.getJSONArray(ES_INDEX);

            assertEquals("INDEX", changes.getJSONObject(0).getString("operation"));

            assertEquals("a", changes.getJSONObject(1).getString("id"));
            assertEquals("INDEX", changes.getJSONObject(1).getString("operation"));

            assertEquals("a", changes.getJSONObject(2).getString("id"));
            assertEquals("DELETE", changes.getJSONObject(2).getString("operation"));

            assertEquals("b", changes.getJSONObject(3).getString("id"));
            assertEquals("INDEX", changes.getJSONObject(3).getString("operation"));
        }
    }
}
