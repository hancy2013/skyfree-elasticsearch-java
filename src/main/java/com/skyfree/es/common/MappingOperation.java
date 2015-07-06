package com.skyfree.es.common;

import com.skyfree.es.client.EsNativeClient;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 15:21
 */
public class MappingOperation {
    private final Client client;

    public MappingOperation(Client client) {
        this.client = client;
    }

    public boolean createMapping(String indexName, String typeName, XContentBuilder builder) {
        PutMappingResponse response = client.admin().indices()
                .preparePutMapping(indexName)
                .setType(typeName)
                .setSource(builder)
                .execute()
                .actionGet();

        return response.isAcknowledged();

    }

    public void deleteMapping(String indexName, String typeName) {
        client.admin().indices().prepareDeleteMapping(indexName).setType(typeName).execute().actionGet();
    }

    public GetMappingsResponse getMapping(String indexName, String typeName) {
        return client.admin().indices()
                .prepareGetMappings(indexName)
                .setTypes(typeName)
                .execute()
                .actionGet();
    }

    public static void main(String[] args) throws IOException {
        Client client = EsNativeClient.getClient();
        MappingOperation mo = new MappingOperation(client);

        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type1")
                .startObject()
                .field("properties")
                .startObject()
                .field("nested1")
                .startObject()
                .field("type")
                .value("nested")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        System.out.println(builder.string());
        mo.createMapping("myindex", "type1", builder);
        
        mo.deleteMapping("myindex","type1");
        System.out.println("done!");
        
    }
}
