package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.plugins.ActionPlugin;
import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

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
	public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
			ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
			IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
		return singletonList(new FollowUpAction(settings, restController));
	}
	
	@Override
	public void onIndexModule(IndexModule indexModule) {
		String indexName = indexModule.getIndex().getName();
		listeners.putIfAbsent(indexName, new IndexListener(indexName));
		indexModule.addIndexOperationListener(listeners.get(indexName));
	}
}
