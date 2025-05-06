package com.cjq.domain;

import com.cjq.common.ElasticsearchJdbcConfig;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;

public class Client {
    private RestHighLevelClient restHighLevelClient;
    private Boolean isClose = false;

    public Client(HttpHost[] httpHosts, Properties properties) {
        String username = properties.getProperty(ElasticsearchJdbcConfig.USERNAME.getName());
        String password = properties.getProperty(ElasticsearchJdbcConfig.PASSWORD.getName());
        String connectTimeout = properties.getProperty(ElasticsearchJdbcConfig.CONNECT_TIMEOUT.getName(),
            ElasticsearchJdbcConfig.CONNECT_TIMEOUT.getDefaultValue());
        String socketTimeout = properties.getProperty(ElasticsearchJdbcConfig.SOCKET_TIMEOUT.getName(),
            ElasticsearchJdbcConfig.SOCKET_TIMEOUT.getDefaultValue());
        if (username != null && password != null) {
            CredentialsProvider credProv = new BasicCredentialsProvider();
            credProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(httpHosts)
                    .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credProv))
                    .setRequestConfigCallback(
                        requestConfigBuilder ->
                            requestConfigBuilder.setConnectTimeout(Integer.parseInt(connectTimeout))
                                .setSocketTimeout(Integer.parseInt(socketTimeout))));
        } else {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        }
    }

    public RestHighLevelClient getClient() {
        return this.restHighLevelClient;
    }

    public Boolean isClose() {
        return isClose;
    }

    public void close() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        isClose = true;
    }
}
