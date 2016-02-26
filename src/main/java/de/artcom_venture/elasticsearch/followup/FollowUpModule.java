package de.artcom_venture.elasticsearch.followup;

import org.elasticsearch.common.inject.AbstractModule;

/**
 * @license MIT
 * @copyright artcom venture GmbH
 * @author Olegs Kunicins
 */
public class FollowUpModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(FollowUpAction.class).asEagerSingleton();
    }
}
