package com.skyfree.es.common;

import com.skyfree.es.client.EsNativeClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 16:53
 */
public class BulkOperation {
    private final Client client;

    public BulkOperation(Client client) {
        this.client = client;
    }

    public int bulkInsert(String indexName, String typeName, List<Map<String, Object>> data) {
        BulkRequestBuilder bulker = client.prepareBulk();
        for (Map<String, Object> item : data) {
            if (item.get("id") == null) {
                return -1;
            }
            bulker.add(client.prepareIndex(indexName, typeName, item.get("id").toString()).setSource(item));
        }

        return bulker.execute().actionGet().getItems().length;
    }

    public int bulkUpdate(String indexName, String typeName, List<String> ids, String field, String value) {
        BulkRequestBuilder bulker = client.prepareBulk();

        for (String id : ids) {
            bulker.add(
                    client.prepareUpdate(indexName, typeName, id)
                            .setScript("ctx._source." + field + " = '" + value + "' ", ScriptService.ScriptType.INLINE)
            );
        }
        return bulker.execute().actionGet().getItems().length;
    }

    public int bulkDelete(String indexName, String typeName, List<String> ids) {
        BulkRequestBuilder bulker = client.prepareBulk();

        for (String id : ids) {
            bulker.add(
                    client.prepareDelete(indexName, typeName, id)
            );
        }
        return bulker.execute().actionGet().getItems().length;
    }

    public static void main(String[] args) throws IOException {
        String index = "mytest";
        String type = "mytype";

        Client client = EsNativeClient.getClient();
        IndicesOperation io = new IndicesOperation(client);

        if (io.checkIndexExists(index)) {
            io.deleteIndex(index);
        }

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("_timestamp").field("enabled",
                        true).field("store", "yes").endObject()
                .startObject("_ttl").field("enabled",
                        true).field("store", "yes").endObject()
                .endObject()
                .endObject();

        io.createIndex(index, type, builder);

        // 初始化结束
        
        int sampleSize = 1000;
        
        List<Map<String, Object>> data = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < sampleSize; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i + 1);
            value.put("name", "value" + i);
            data.add(i, value);
        }

        BulkOperation bo = new BulkOperation(client);
        int insertCount = bo.bulkInsert(index, type, data);
        System.out.println("sample count:" + insertCount);
        long end = System.currentTimeMillis();
        System.out.println("insert:" + (end - start) + "ms");

        List<String> ids = new ArrayList<>();

        for (int i = 0; i < sampleSize; i++) {
            ids.add("" + (i + 1));
        }

        start = System.currentTimeMillis();
        int updateCount = bo.bulkUpdate(index, type, ids, "name", "baotingfang");
        end = System.currentTimeMillis();
        System.out.println("update:" + (end - start) + "ms");

//        start = System.currentTimeMillis();
//        int deletedCount = bo.bulkDelete(index, type, ids);
//        end = System.currentTimeMillis();
//        System.out.println("delete:" + (end - start) + "ms");


    }
}
