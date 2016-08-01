package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpPlugin extends Plugin {
	
	@Override 
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new FollowUpModule());
    }

	@Override 
	public String description() {
		return "FollowUp Plugin";
	}

	@Override 
	public String name() {
		return "followup";
	}
}
