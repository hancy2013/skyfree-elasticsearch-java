package com.skyfree.es.client;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 14:25
 */

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.*;

public class EsNativeClient {

    private static Client nodeClient;
    private static Client client;

    static {
        // nodeClient = createNodeClient();
        client = createTransportClient();
    }

    private static Client createNodeClient() {
        Node node = nodeBuilder().clusterName("skyfree-es").client(true).node();
        return node.client();
    }

    public void closeNodeClient() {
        if (nodeClient != null) {
            nodeClient.close();
        }
    }

    private static Client createTransportClient() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .put("cluster.name", "skyfree-es").build();

        return new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("skyfree1", 9300));
    }

    public static Client getClient() {
        return client;
    }

}
