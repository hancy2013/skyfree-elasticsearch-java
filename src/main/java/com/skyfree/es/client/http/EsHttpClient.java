package com.skyfree.es.client.http;

import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/3 10:55
 */
public class EsHttpClient {
    private static String serverName = "skyfree1";
    private static int serverPort = 9200;
    private static String userName = "";
    private static String passWord = "";

    private static String esUrl = "http://skyfree1:9200/";

    private static HttpClient client;
    private static HttpClientContext context;

    static {
        client = createHttpClient();
        context = createContext(serverName, serverPort, "http", userName, passWord);
    }

    private static HttpClient createHttpClient() {

        // 添加了重试3次的逻辑
        CloseableHttpClient client = HttpClients.custom().setRetryHandler(new HttpRequestRetryHandler() {
            public boolean retryRequest(IOException e, int i, HttpContext httpContext) {
                return i <= 3;
            }
        }).build();

        return client;
    }
    

    // 主要是启用身份认证的作用,es节点上可以结合elasticsearch-jetty使用:https://github.com/sonian/elasticsearch-jetty
    private static HttpClientContext createContext(String serverName, int port, String schema, String userName, String passWord) {
        HttpHost targetHost = new HttpHost(serverName, port, schema);

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(userName, passWord));

        AuthCache authCache = new BasicAuthCache();
        BasicScheme basicAuth = new BasicScheme();
        authCache.put(targetHost, basicAuth);

        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(credentialsProvider);

        return context;
    }

    public String get(String relativePath) {
        HttpGet method = new HttpGet(esUrl + relativePath);
        try {
            HttpResponse response = client.execute(method);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                System.err.println("Method failed:" + response.getStatusLine());
            } else {
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args) {
        EsHttpClient client = new EsHttpClient();
        String ret = client.get("");
        System.out.println(ret);
    }
}
