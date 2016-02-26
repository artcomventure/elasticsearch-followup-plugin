package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.index.engine.Engine.Operation.Type;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class Change {
    public final Type operation;
    public final String type;
    public final String id;

    public Change(Type operation, String id, String type) {
    	this.operation = operation;
    	this.id = id;
    	this.type = type;
    }
}
