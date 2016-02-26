package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpPlugin extends AbstractPlugin {
	
	@Override 
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        modules.add(FollowUpModule.class);
        return modules;
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
