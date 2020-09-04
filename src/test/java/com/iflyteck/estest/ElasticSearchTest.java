package com.iflyteck.estest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iflyteck.es.Article;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class ElasticSearchTest {

    /**
     * 使用集群版ElasticSearch创建
     * @throws Exception
     */
    @Test
    public void createIndex () throws Exception {
        // 创建一个Settings对象,相当于是一个配置信息，主要配置集群的名称
        Settings settings = Settings.builder()
                .put("cluster.name", "my-elasticsearch")
                .build();
        // 创建一个客户端Client对象
        PreBuiltTransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9303));
        // 使用client对象创建一个索引库
        client.admin().indices().prepareCreate("es-hello").get();
        // 关闭client对象
        client.close();
    }

    /**
     * 使用单一版ElasticSearch创建索引
     * @throws Exception
     */
    @Test
    public void createIndexSignal () throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        client.admin().indices().prepareCreate("es-singal").get();
        client.close();
    }

    @Test
    public void setMappins() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","text")
                                .field("store",true)
                            .endObject()
                            .startObject("title")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        client.admin().indices()
                // 设置要做映射的索引
                .preparePutMapping("es-hello")
                // 设置要做映射的type
                .setType("article")
                // mapping信息，可以是XContentBuilder也可以是JSON格式的字符串
                .setSource(xContentBuilder)
                .get();
        client.close();
    }

    @Test
    public void testAddDocuments() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 3l)
                .field("title", "今天如何纪念我们的胜利日？习近平这样说")
                .field("content", "又是一年9月3日，距离那场伟大的胜利已有75个年头。但今年这一天，胜利日所激发的力量却更加澎湃。上午，习近平总书记向抗战烈士敬献花篮，下午，出席座谈会并发表重要讲话。透过他的行动、他的讲话，人们感受到了这场纪念的非同寻常。")
                .endObject();
        client.prepareIndex()
                .setIndex("es-hello")
                .setType("article")
                .setId("3")
                .setSource(xContentBuilder)
                .get();
        client.close();
    }

    @Test
    public void addDocumentByDomain() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        Article article = new Article();
        article.setId(4l);
        article.setTitle("特殊的日子，听习近平解答这个时代命题");
        article.setContent("2020年9月3日是中国人民抗日战争暨世界反法西斯战争胜利75周年纪念日。今天上午，习近平等党和国家领导人同首都各界代表一起，在中国人民抗日战争纪念馆向抗战烈士敬献花篮。");
        ObjectMapper objectMapper = new ObjectMapper();
        String articleStr = objectMapper.writeValueAsString(article);
        client.prepareIndex("es-hello","article","4")
                .setSource(articleStr, XContentType.JSON)
                .get();
//        client.prepareIndex()
//                .setIndex("index_hello")
//                .setType("article")
//                .setId("1")
//                .setSource(articleStr)
//                .get();
        client.close();
    }

    // 向es中写入100条数据
    @Test
    public void addDocument3() throws Exception{
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        for (int i = 0; i < 100; i++) {
            Article article = new Article();
            article.setId((long) i);
            article.setTitle("特殊的日子，听习近平解答这个时代命题");
            article.setContent("2020年9月3日是中国人民抗日战争暨世界反法西斯战争胜利75周年纪念日。今天上午，习近平等党和国家领导人同首都各界代表一起，在中国人民抗日战争纪念馆向抗战烈士敬献花篮。");
            ObjectMapper objectMapper = new ObjectMapper();
            String articleStr = objectMapper.writeValueAsString(article);
            client.prepareIndex("es-hello","article",i+"")
                    .setSource(articleStr, XContentType.JSON)
                    .get();
        }
        client.close();
    }

}
