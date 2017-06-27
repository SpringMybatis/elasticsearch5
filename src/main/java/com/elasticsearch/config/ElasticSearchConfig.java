package com.elasticsearch.config;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2017/6/27.
 */
@Component
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchConfig {
    private String clusterName;
    private String clusterNodes;

    @Bean
    public Client getESClient() {
        // 设置集群名字
        Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();
        Client client = new PreBuiltTransportClient(settings);
        try {
            // 读取的ip列表是以逗号分隔的
            for (String clusterNode : this.clusterNodes.split(",")) {
                String ip = clusterNode.split(":")[0];
                String port = clusterNode.split(":")[1];
                ((TransportClient) client).addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(ip), Integer.parseInt(port)));
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }
}
