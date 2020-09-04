import com.iflyteck.es.Article;
import com.iflyteck.es.repositories.ArticleRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Optional;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class SpringDataElasticSearchTest {
    @Autowired
    private ArticleRepository articleRepository;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Test
    public void testCreateIndex() {
        boolean res = elasticsearchTemplate.createIndex(Article.class);
        if (res) {
            System.out.println("创建成功");
        }
    }

    /**
     * 新增一条数据
     */
    @Test
    public void testAddDocument() {
        Article article = new Article();
        article.setId(1l);
        article.setTitle("es之java简单创建索引333");
        article.setContent("ElasticSearchhahah客户端提供了多种方式的数据创建方式，包括json串,map,内置工具；我们正式开始一般用json格式，借助json工具框架，比如gson ,json-lib,fastjson等等；");
        articleRepository.save(article);
    }

    /**
     * 删除一条数据
     */
    @Test
    public void testDeleteDocument() {
        articleRepository.deleteById(1l);
    }

    /**
     * 没有更新的方法，因为ElasticSearch更新的原理是先删除后更新，所以如果要更新就直接重新新增一遍即可
     */

    @Test
    public void testQueryEs() {
        Iterable<Article> articles = articleRepository.findAll();
        articles.forEach(article -> System.out.println(article));
    }

    @Test
    public void testFindById() {
        Optional<Article> opt = articleRepository.findById(1l);
        Article article = opt.get();
        System.out.println(article);
    }

    /**
     * 自定义查询
     */
    @Test
    public void testFindByTitle() {
        List<Article> articles = articleRepository.findByTitle("创建");
        articles.forEach(article -> System.out.println(article));
    }

    @Test
    public void testFindByTitleOrContent() {
        List<Article> articles = articleRepository.findByTitleOrContent("啦啦", "创建");
        articles.forEach(article -> System.out.println(article));
    }
    @Test
    public void testFindByTitleOrContentPageAble() {
        Pageable pageable = PageRequest.of(0,5);
        List<Article> articles = articleRepository.findByTitleOrContent("啦啦", "创建",pageable);
        articles.forEach(article -> System.out.println(article));
    }

    /**
     * 原生的es的查询
     */
    @Test
    public void testNativeQuery() {
        NativeSearchQuery nativeSearchQueryBuilder = new NativeSearchQueryBuilder()
                .withQuery(QueryBuilders.queryStringQuery("java语言").defaultField("title"))
                .withPageable(PageRequest.of(0,15))
                .build();
        // 执行查询
        List<Article> articles = elasticsearchTemplate.queryForList(nativeSearchQueryBuilder, Article.class);
        articles.forEach(article -> System.out.println(article));
    }
}
