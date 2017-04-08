package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.plugins.ActionPlugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpPlugin extends Plugin implements ActionPlugin {
	
	private static final Map<String, IndexListener> listeners = new ConcurrentHashMap<>();
	
	public static IndexListener getListener(String indexName) {
		return listeners.get(indexName);
	}
	
	@Override
	public List<Class<? extends RestHandler>> getRestHandlers() {
		return Collections.singletonList(FollowUpAction.class);
    }
	
	@Override
	public void onIndexModule(IndexModule indexModule) {
		String indexName = indexModule.getIndex().getName();
		listeners.putIfAbsent(indexName, new IndexListener(indexName));
		indexModule.addIndexOperationListener(listeners.get(indexName));
	}
}
