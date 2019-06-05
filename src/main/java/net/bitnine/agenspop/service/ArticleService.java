package net.bitnine.agenspop.service;

import net.bitnine.agenspop.dto.Article;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.Optional;

public interface ArticleService {
    Article save(Article article);

    Optional<Article> findOne(String id);

    Iterable<Article> findAll();

    Page<Article> findByAuthorName(String name, Pageable pageable);

    Page<Article> findByAuthorNameUsingCustomQuery(String name, Pageable pageable);

    Page<Article> findByFilteredTagQuery(String tag, Pageable pageable);

    Page<Article> findByAuthorsNameAndFilteredTagQuery(String name, String tag, Pageable pageable);

    long count();

    void delete(Article article);
}