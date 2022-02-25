package de.artcom_venture.elasticsearch.followup;

import junit.framework.TestCase;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Random;

public class FollowUpPluginTest extends TestCase {

    private RestClient client;
    private static Random randomGenerator = new Random();
    private static final String ES_INDEX = "myindex";
    private static final String PLUGIN_NAME = "followup";
    private static final String HOST = "127.0.0.1";
    private static final String PLUGIN_URL_LIST = "/" + ES_INDEX + "/_" + PLUGIN_NAME;

    @Override
    public void setUp() throws Exception {
        client = RestClient.builder(new HttpHost(HOST, 9200)).build();

        // create index
        try {
            Request request = new Request("DELETE", "/" + ES_INDEX);
            client.performRequest(request);
        } catch (ResponseException ignored) {
        }
        Request request = new Request("PUT", "/" + ES_INDEX);
        request.setJsonEntity("{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{}}}");
        client.performRequest(request);

        // create initial document
        clearListener();
        indexDocument("1");
    }

    @Override
    public void tearDown() throws Exception {
        client.close();
    }

    private int getChangesLength() throws IOException {
        Request request = new Request("GET", PLUGIN_URL_LIST);
        Response response = client.performRequest(request);
        JSONObject json = new JSONObject(EntityUtils.toString(response.getEntity()));
        return json.getJSONArray(ES_INDEX).length();
    }

    private int stopListener() throws IOException {
        Request request = new Request("GET", PLUGIN_URL_LIST + "?stop");
        Response response = client.performRequest(request);
        return response.getStatusLine().getStatusCode();
    }

    private int startListener() throws IOException {
        Request request = new Request("GET", PLUGIN_URL_LIST + "?start");
        Response response = client.performRequest(request);
        return response.getStatusLine().getStatusCode();
    }

    private int clearListener() throws IOException {
        Request request = new Request("GET", PLUGIN_URL_LIST + "?clear");
        Response response = client.performRequest(request);
        return response.getStatusLine().getStatusCode();
    }

    private void indexDocument(String id) throws IOException {
        Request request = new Request("PUT", "/" + ES_INDEX + "/_doc/" + id);
        request.setJsonEntity("{\"key\": \"Lorem ipsum\", \"value\": \"" + randomGenerator.nextLong() + "\"}");
        client.performRequest(request);
    }

    private void deleteDocument(String id) throws IOException {
        Request request = new Request("DELETE", "/" + ES_INDEX + "/_doc/" + id);
        client.performRequest(request);
    }

    private void createDocument() throws IOException {
        Request request = new Request("POST", "/" + ES_INDEX + "/_doc/");
        request.setJsonEntity("{\"key\": \"Lorem ipsum\", \"value\": \"" + randomGenerator.nextLong() + "\"}");
        client.performRequest(request);
    }

    private String getPluginVersion() throws IOException {
        Request request = new Request("GET", "/_cat/plugins?h=component,version&format=json");
        Response response = client.performRequest(request);
        JSONArray json = new JSONArray(EntityUtils.toString(response.getEntity()));
        for (Object pluginInfo : json) {
            if (((JSONObject) pluginInfo).getString("component").equals(PLUGIN_NAME)) {
                return ((JSONObject) pluginInfo).getString("version");
            }
        }
        return null;
    }

    private String getServerVersion() throws IOException {
        Request request = new Request("GET", "/");
        Response response = client.performRequest(request);
        JSONObject json = new JSONObject(EntityUtils.toString(response.getEntity()));
        return json.getJSONObject("version").getString("number");
    }

    public void testCompatibility() throws IOException {
        assertTrue(getPluginVersion().startsWith(getServerVersion()));
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

        Request request = new Request("GET", PLUGIN_URL_LIST);
        Response response = client.performRequest(request);
        JSONObject json = new JSONObject(EntityUtils.toString(response.getEntity()));
        JSONArray changes = json.getJSONArray(ES_INDEX);

        assertEquals("INDEX", changes.getJSONObject(0).getString("operation"));

        assertEquals("a", changes.getJSONObject(1).getString("id"));
        assertEquals("INDEX", changes.getJSONObject(1).getString("operation"));

        assertEquals("a", changes.getJSONObject(2).getString("id"));
        assertEquals("DELETE", changes.getJSONObject(2).getString("operation"));

        assertEquals("b", changes.getJSONObject(3).getString("id"));
        assertEquals("INDEX", changes.getJSONObject(3).getString("operation"));
    }
}
