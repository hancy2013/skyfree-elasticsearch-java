package com.skyfree.es.common;

import com.skyfree.es.client.EsNativeClient;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 15:43
 */
public class DocOperation {
    private final Client client;

    public DocOperation(Client client) {
        this.client = client;
    }

    /**
     * 适合扁平化对象的插入操作, 这里至少解决了没有层次的key-value的问题
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param id        doc_id
     * @param values    文档值
     * @return 是否成功插入
     */
    public boolean insert(String indexName, String typeName, String id, Map<String, Object> values) {
        IndexResponse response = client.prepareIndex(indexName, typeName, id)
                .setSource(values)
                .execute()
                .actionGet();

        return response.isCreated();
    }

    public Map<String, Object> get(String indexName, String typeName, String id) {
        GetResponse response = client.prepareGet(indexName, typeName, id).execute().actionGet();
        return response.getSourceAsMap();
    }

    public void update(String indexName, String typeName, String id, String field, String value) {
        client.prepareUpdate(indexName, typeName, id)
                .setScript("ctx._source." + field + " = '" + value + "' ", ScriptService.ScriptType.INLINE)
                .execute()
                .actionGet();
    }

    public void delete(String indexName, String typeName, String id) {
        client.prepareDelete(indexName, typeName, id).execute().actionGet();
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
        DocOperation docOperation = new DocOperation(client);

        Map<String, Object> map = new HashMap<>();
        map.put("name", "bao");
        map.put("city", "beijing");

        System.out.println(docOperation.insert(index, type, "0001", map));

        io.refreshIndex(index);

        Map ret = docOperation.get(index, type, "0001");
        System.out.println(ret);
        System.out.println(ret.get("city"));

        docOperation.update(index, type, "0001", "city", "jincheng1");

        ret = docOperation.get(index, type, "0001");
        System.out.println(ret);

        docOperation.delete(index, type, "0001");

    }
}
