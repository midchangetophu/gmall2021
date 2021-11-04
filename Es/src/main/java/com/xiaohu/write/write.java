package com.xiaohu.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class write {
    public static void main(String[] args) throws IOException {
        //创建es客户端构造器
        JestClientFactory jestClientFactory = new JestClientFactory();
        //创建客户端链接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        //设置es连接地址
        jestClientFactory.setHttpClientConfig(httpClientConfig);
        //获取客户端连接对象
        JestClient jestClient= jestClientFactory.getObject();
        //构造es插入数据对象
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"333\",\n" +
                "  \"name\":\"zhang3\"\n" +
                "}\n").index("movie_test2").type("_doc").id("1002").build();
        //执行插入操作
        jestClient.execute(index);

    }
}
