package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpAction extends BaseRestHandler {

	private static final String STATUS = "status";
	
	@Inject
	public FollowUpAction(Settings settings, RestController controller) {
	}

	@Override
	protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
		String indexName = restRequest.param("index");

		if (FollowUpPlugin.getListener(indexName) == null) {
			return channel -> channel.sendResponse(new RestResponse(NOT_FOUND, channel.newBuilder()));
		}
		
		if (restRequest.param("start", null) != null) {
			FollowUpPlugin.getListener(indexName).start();
			return channel -> channel.sendResponse(new RestResponse(OK, channel.newBuilder().startObject().field(STATUS, 200).endObject()));
		}
		if (restRequest.param("stop", null) != null) {
			FollowUpPlugin.getListener(indexName).stop();
			return channel -> channel.sendResponse(new RestResponse(OK, channel.newBuilder().startObject().field(STATUS, 200).endObject()));
		}
		if (restRequest.param("clear", null) != null) {
			FollowUpPlugin.getListener(indexName).clear();
			return channel -> channel.sendResponse(new RestResponse(OK, channel.newBuilder().startObject().field(STATUS, 200).endObject()));
		}
		return channel -> {
			XContentBuilder builder = channel.newBuilder();
			builder.startObject();
			IndexListener indexListener = FollowUpPlugin.getListener(indexName);
			if (indexListener != null) {
				builder.startArray(indexName);
				for (Change change : indexListener.getChanges()) {
					builder.startObject();
					builder.field("operation", change.operation);
					builder.field("type", change.type);
					builder.field("id", change.id);
					builder.endObject();
				}
				builder.endArray();
			}
			builder.endObject();
			channel.sendResponse(new RestResponse(OK, builder));
		};
	}

    @Override
    public String getName() {
        return "FollowUp";
    }

	@Override
	public List<Route> routes() {
		return Arrays.asList(new Route(GET, "/{index}/_followup"));
	}
}
