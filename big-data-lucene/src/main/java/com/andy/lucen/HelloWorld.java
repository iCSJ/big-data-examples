package com.andy.lucen;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * <p> lucene测试
 *
 * @author Leone
 * @since 2018-11-22
 **/
public class HelloWorld {


    public static void main(String[] args) throws IOException {
        Article article = new Article();
        article.setId(10006L);
        article.setAuthor("赵兴");
        article.setTitle("标题");
        article.setContent("如何中奖");
        article.setUrl("http://www.baidu.com");

        String indexPath = "d:/tmp/words";

        FSDirectory fsDirectory = FSDirectory.open(Paths.get(indexPath));
        // 分词器 标准分词器一个字分一次
        Analyzer analyzer = new StandardAnalyzer();

        // 写入索引的配置，设置分词器
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        // 指定了写入数据目录和配置
        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexWriterConfig);

        Document document = article.toDocument();
        indexWriter.addDocument(document);
        indexWriter.close();


    }


}
