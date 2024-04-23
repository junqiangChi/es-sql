package com.cjq.domain;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {

  private String username;
  private String password;
  private String hostname;
  private int port;

  public Client() {

  }

  public Client(String username, String password, String hostname) {
    this.username = username;
    this.password = password;
    this.hostname = hostname;
    this.port = 9200;
  }

  public Client(String username, String password, String hostname, int port) {
    this.username = username;
    this.password = password;
    this.hostname = hostname;
    this.port = port;
  }

  public RestHighLevelClient getClient() {
    CredentialsProvider credProv = new BasicCredentialsProvider();
    credProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    return new RestHighLevelClient(RestClient.builder(
            new HttpHost(hostname, port, "http")).
        setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
            .setDefaultCredentialsProvider(credProv)));
  }


  public void close() {
  }
}
