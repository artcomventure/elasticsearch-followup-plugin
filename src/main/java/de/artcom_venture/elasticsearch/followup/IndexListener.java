package de.artcom_venture.elasticsearch.followup;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.engine.Engine.DeleteResult;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class IndexListener implements IndexingOperationListener {
	private static final Logger LOG = LogManager.getLogger(FollowUpPlugin.class);
	private ConcurrentLinkedQueue<Change> changes = new ConcurrentLinkedQueue<>();
	private boolean isStarted = false;
	private final String indexName;
	
	public IndexListener(String indexName) {
		this.indexName = indexName;
		LOG.info("[followup] follows [" + indexName + "]");
	}
	
	@Override
	public void postDelete(ShardId shardId, Delete document, DeleteResult result) {
		if (this.isStarted) {
			changes.add(new Change(Change.DELETE, document.id()));
		}
	}

	@Override
	public void postIndex(ShardId shardId, Index document, IndexResult result) {
		if (this.isStarted) {
			changes.add(new Change(Change.INDEX, document.id()));
		}
	}
	
	public Change[] getChanges() {
		return changes.toArray(new Change[0]);
	}
	
	public void start() {
		this.changes = new ConcurrentLinkedQueue<>();
		this.isStarted = true;
		LOG.info("[followup] started listening on [" + this.indexName + "]");
	}
	
	public void stop() {
		this.isStarted = false;
		LOG.info("[followup] stopped listening on [" + this.indexName + "]");
	}
	
	public void clear() {
		this.changes.clear();
		LOG.info("[followup] cleared of [" + this.indexName + "]");
	}
}
