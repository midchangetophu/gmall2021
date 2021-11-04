package com.xiaohu.write;

import com.xiaohu.bean.Movie;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;


import java.io.IOException;

public class writeall {
    public static void main(String[] args) throws IOException {
        //创建客户端对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        //设置连接参数
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        Movie movie1 = new Movie("1002", "张三");
        Movie movie2 = new Movie("1003", "三");
        Movie movie3 = new Movie("1004", "张");
        Index index1 = new Index.Builder(movie1).id("1002").build();
        Index index2 = new Index.Builder(movie2).id("1003").build();
        Index index3 = new Index.Builder(movie3).id("1004").build();
        Bulk bulk = new Bulk.Builder().defaultIndex("movie_test2").
                defaultType("_doc").addAction(index1).addAction(index2).addAction(index3).build();
        jestClient.execute(bulk);
        jestClient.shutdownClient();
        //执行插入操作

    }

}
