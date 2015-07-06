package com.skyfree.es.common;

import com.skyfree.es.client.EsNativeClient;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 14:38
 */
public class IndicesOperation {
    private final Client client;

    public IndicesOperation(Client client) {
        this.client = client;
    }

    public boolean checkIndexExists(String indexName) {
        IndicesExistsResponse response = client.admin().indices().prepareExists(indexName).execute().actionGet();
        return response.isExists();
    }

    public void createIndex(String indexName) {
        this.client.admin().indices().prepareCreate(indexName).execute().actionGet();
    }

    /**
     * 创建索引时,就指定了mapping
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param builder   mapping构造器
     */
    public void createIndex(String indexName, String typeName, XContentBuilder builder) {
        client.admin().indices().prepareCreate(indexName).addMapping(typeName, builder).execute().actionGet();
    }

    public void deleteIndex(String indexName) {
        client.admin().indices().prepareDelete(indexName).execute().actionGet();
    }

    public void closeIndex(String indexName) {
        client.admin().indices().prepareClose(indexName).execute().actionGet();
    }

    public void openIndex(String indexName) {
        client.admin().indices().prepareOpen(indexName).execute().actionGet();
    }

    public void optimizeIndex(String indexName) {
        client.admin().indices().prepareOptimize(indexName).execute().actionGet();
    }

    public void flushIndex(String indexName) {
        client.admin().indices().prepareFlush(indexName).execute().actionGet();
    }

    public void refreshIndex(String indexName) {
        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
    }

    public static void main(String[] args) {
        Client client = EsNativeClient.getClient();
        IndicesOperation indexOperation = new IndicesOperation(client);

        String indexName = "test_index";

        if (!indexOperation.checkIndexExists(indexName)) {
            indexOperation.createIndex(indexName);
            System.out.println("created index!");
        }

        System.out.println(indexOperation.checkIndexExists(indexName));
        indexOperation.openIndex(indexName);
        indexOperation.closeIndex(indexName);

        indexOperation.openIndex(indexName);
        // index必须是open的状态才能flush, optimize操作
        indexOperation.flushIndex(indexName);
        indexOperation.optimizeIndex(indexName);

        if (indexOperation.checkIndexExists(indexName)) {
            indexOperation.deleteIndex(indexName);
        }

        System.out.println("done!");
    }
}
