package de.artcom_venture.elasticsearch.followup;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import java.util.concurrent.ConcurrentHashMap;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesLifecycle.Listener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpAction extends BaseRestHandler {
	
	private static final ESLogger LOG = Loggers.getLogger(FollowUpAction.class);
	private ConcurrentHashMap<String, IndexListener> listeners = new ConcurrentHashMap<String, IndexListener>();

	@Inject
	public FollowUpAction(Settings settings, Client client, RestController controller, IndicesService indicesService) {
		super(settings, controller, client);
		
		controller.registerHandler(GET, "/{index}/_followup", this);
		indicesService.indicesLifecycle().addListener(new Listener() {
			@Override
			public void afterIndexShardStarted(IndexShard indexShard) {
				String indexName = indexShard.shardId().index().name();
				listeners.putIfAbsent(indexName, new IndexListener());			
				LOG.info("[followup] follows [" + indexName + "]");
				indexShard.indexingService().addListener(listeners.get(indexName));
			}
		});
	}
	
	@Override
	protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
		String indexName = request.param("index");
		XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
		
		if (!listeners.containsKey(indexName)) {
			channel.sendResponse(new BytesRestResponse(NOT_FOUND, builder));
			return;
		}
		
		builder.startObject();
		
		if (request.param("start", null) != null) {
			listeners.get(indexName).start();
			builder.field("status", 200);
			LOG.info("[followup] started listening on [" + indexName + "]");
		} else if (request.param("stop", null) != null) {
			listeners.get(indexName).stop();
			builder.field("status", 200);
			LOG.info("[followup] stopped listening on [" + indexName + "]");
		} else if (request.param("clear", null) != null) {
			listeners.get(indexName).clear();
			builder.field("status", 200);
			LOG.info("[followup] cleared of [" + indexName + "]");
		} else {
			IndexListener indexListener = listeners.get(indexName);
			if (indexListener != null) {
				builder.startArray(indexName);
				for (Change change : indexListener.getChanges()) {
					builder.startObject();
					builder.field("operation", change.operation.toString());
					builder.field("type", change.type);
					builder.field("id", change.id);
					builder.endObject();
				}
				builder.endArray();
			}
		}
		
		builder.endObject();
		channel.sendResponse(new BytesRestResponse(OK, builder));
	}
}
