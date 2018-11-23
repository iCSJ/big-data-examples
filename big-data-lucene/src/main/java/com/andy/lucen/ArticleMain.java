package com.andy.lucen;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.FileSystems;

/**
 * <p> lucene测试
 *
 * @author Leone
 * @since 2018-11-22
 **/
public class ArticleMain {

    private static String dirPath = "D:\\lucene\\docDir";

    private static String indexPath = "D:\\lucene\\indexDir";

    public static void main(String[] args) throws Exception {
        Article article = new Article(10006L, "张三", "标题", "湖人总冠军", "http://www.baidu.com");

        Analyzer analyzer = new SimpleAnalyzer();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);

        FSDirectory fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath(indexPath));

        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexWriterConfig);

        // 清除以前的index
        indexWriter.deleteAll();

        indexWriter.addDocument(article.toDocument());

        indexWriter.close();

    }


}
