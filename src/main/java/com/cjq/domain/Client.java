package com.cjq.domain;

import com.cjq.common.EsJdbcConfig;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class Client {
    private RestHighLevelClient restHighLevelClient;

    public Client() {
    }

    public void init(String[] hostAndPorts, Properties properties) {
        String username = properties.getProperty(EsJdbcConfig.USERNAME);
        String password = properties.getProperty(EsJdbcConfig.PASSWORD);
        HttpHost[] httpHosts = Arrays.stream(hostAndPorts).map(host -> {
            String[] hostAndPort = host.split(":");
            return new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1]), "http");
        }).toArray(HttpHost[]::new);
        if (username != null && password != null) {
            CredentialsProvider credProv = new BasicCredentialsProvider();
            credProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts)
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credProv)));
        } else {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHosts));
        }
    }

    public RestHighLevelClient getClient() {
        return this.restHighLevelClient;
    }


    public void close() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
