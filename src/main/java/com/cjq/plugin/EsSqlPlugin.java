package com.cjq.plugin;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Elasticsearch SQL Plugin
 * Main plugin class that integrates SQL functionality into Elasticsearch
 * Provides REST endpoints for SQL query execution and web interface
 */
public class EsSqlPlugin extends Plugin implements ActionPlugin {
    
    /**
     * Default constructor for the ES SQL Plugin
     */
    public EsSqlPlugin() {
    }

    /**
     * Registers REST handlers for the plugin
     * Creates and returns the REST endpoints that will be available in Elasticsearch
     * 
     * @param settings the plugin settings
     * @param restController the REST controller for registering handlers
     * @param clusterSettings the cluster settings
     * @param indexScopedSettings the index-scoped settings
     * @param settingsFilter the settings filter
     * @param indexNameExpressionResolver the index name expression resolver
     * @param nodesInCluster supplier for discovery nodes in the cluster
     * @return list of REST handlers for the plugin
     */
    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new EsSqlRestAction(), new EsPluginWebAction());
    }
}
