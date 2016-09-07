package de.artcom_venture.elasticsearch.followup;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.elasticsearch.index.engine.Engine.Create;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.indexing.IndexingOperationListener;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class IndexListener extends IndexingOperationListener {
	private ConcurrentLinkedQueue<Change> changes = new ConcurrentLinkedQueue<Change>();
	private boolean isStarted = false;

	@Override
	public void postCreate(Create document) {
		if (this.isStarted) {
			changes.add(new Change(document.opType(), document.id(), document.type()));	
		}
	}

	@Override
	public void postDelete(Delete document) {
		if (this.isStarted) {
			changes.add(new Change(document.opType(), document.id(), document.type()));
		}
	}

	@Override
	public void postIndex(Index document, boolean created) {
		if (this.isStarted) {
			changes.add(new Change(document.opType(), document.id(), document.type()));
		}
	}
	
	public Change[] getChanges() {
		return changes.toArray(new Change[0]);
	}
	
	public void start() {
		this.changes = new ConcurrentLinkedQueue<Change>();
		this.isStarted = true;
	}
	
	public void stop() {
		this.isStarted = false;
	}
	
	public void clear() {
		this.changes.clear();
	}
}
