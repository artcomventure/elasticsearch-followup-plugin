package de.artcom_venture.elasticsearch.followup;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class Change {
	static final String DELETE = "DELETE";
	static final String INDEX = "INDEX";
    final String operation;
    final String type;
    final String id;

    Change(String operation, String id, String type) {
    	this.operation = operation;
    	this.id = id;
    	this.type = type;
    }
    Change(String operation, String id) {
        this.operation = operation;
        this.id = id;
        this.type = "_doc";
    }
}
