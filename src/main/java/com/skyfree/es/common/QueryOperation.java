package com.skyfree.es.common;

import com.skyfree.es.client.EsNativeClient;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/6 11:12
 */
public class QueryOperation {
    // 这里只是一个演示的例子, 没有什么封装
    public static void main(String[] args) throws IOException {
        String index = "mytest";
        String type = "mytype";

        Client client = EsNativeClient.getClient();
        IndicesOperation io = new IndicesOperation(client);

        if (io.checkIndexExists(index)) {
            io.deleteIndex(index);
        }

        int sampleSize = 1000;

        List<Map<String, Object>> data = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < sampleSize; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", i + 1);
            value.put("name", "value" + i);
            value.put("count", i);
            data.add(i, value);
        }

        BulkOperation bo = new BulkOperation(client);
        int insertCount = bo.bulkInsert(index, type, data);
        System.out.println("sample count:" + insertCount);
        long end = System.currentTimeMillis();
        System.out.println("insert:" + (end - start) + "ms");
        // 必须刷新,搜索搜索不到
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        // 初始化结束

        TermFilterBuilder filter = termFilter("name", "value300");
        RangeQueryBuilder range = rangeQuery("count").gt(200);
        BoolQueryBuilder bool = boolQuery().must(range);

        FilteredQueryBuilder query = filteredQuery(bool, filter);

        SearchResponse response = client.prepareSearch(index).setTypes(type).setQuery(query).execute().actionGet();
        System.out.println("Matched records of elements:" + response.getHits().getTotalHits());

    }
}
