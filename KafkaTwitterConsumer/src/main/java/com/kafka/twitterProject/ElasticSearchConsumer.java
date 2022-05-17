package com.kafka.twitterProject;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){
        String hostname = "";
        String username = "";
        String password = "";

        //don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();



    }

    public static void main(String[] args) {

    }
}
