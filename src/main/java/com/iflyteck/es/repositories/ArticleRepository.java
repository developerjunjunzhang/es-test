package com.iflyteck.es.repositories;

import com.iflyteck.es.Article;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ArticleRepository extends ElasticsearchRepository<Article,Long> {
    List<Article> findByTitle(String title);
    List<Article> findByTitleOrContent(String title,String content);
    List<Article> findByTitleOrContent(String title, String content, Pageable pageable);
}
