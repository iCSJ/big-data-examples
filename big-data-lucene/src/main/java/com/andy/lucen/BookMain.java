package com.andy.lucen;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *
 * @author: Leone
 * @since: 2018-11-23
 **/
public class BookMain {

    private static List<Book> books = new ArrayList<>();

    private static String dirPath = "D:\\lucene\\docDir";
    private static String indexPath = "D:\\lucene\\indexDir";

    static {
        books.add(new Book(1, "java核心编程思想", 23, "http://www.baidu.com/image.jpg", "java 是一个面向对象的语言"));
        books.add(new Book(2, "mysql数据库设计", 12, "http://www.baidu.com/image.jpg", "mysql是一个开源免费的中小型数据库"));
        books.add(new Book(3, "javaWEB进阶", 75, "http://www.baidu.com/image.jpg", "javaWEB核心编程技术大全"));
        books.add(new Book(4, "spring入门", 19, "http://www.baidu.com/image.jpg", "spring 从入门到放弃需要多久"));
        books.add(new Book(5, "springCloud 微服务实战", 42, "http://www.baidu.com/image.jpg", "微服务方案一站式解决方案spring 和 spring cloud"));
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
//        createIndex();
        indexSearch("description:spring");
        System.out.println("一共花费了:" + (System.currentTimeMillis() - start) + "毫秒！");
    }

    private static void createIndex() throws IOException {
        // 将采集到的数据封装到Document对象中
        List<Document> docList = new ArrayList<>();
        Document document;
        for (Book book : books) {
            document = new Document();
            // store:如果是yes，则说明存储到文档域中
            // 图书ID

            Field bookId = new TextField("book_id", Integer.toString(book.getBookId()), Field.Store.YES);
            // 图书名称
            Field name = new TextField("name", book.getName(), Field.Store.YES);
            // 图书价格
            Field price = new TextField("price", Float.toString(book.getPrice()), Field.Store.YES);
            // 图书图片地址
            Field picture = new TextField("picture", book.getPicture(), Field.Store.YES);
            // 图书描述
            Field description = new TextField("description", book.getDescription(), Field.Store.YES);
            // 将field域设置到Document对象中
            document.add(bookId);
            document.add(name);
            document.add(price);
            document.add(picture);
            document.add(description);
            docList.add(document);
        }

        // 创建分词器，标准分词器
        Analyzer analyzer = new StandardAnalyzer();

        // 创建IndexWriter
        IndexWriterConfig cfg = new IndexWriterConfig(analyzer);

        // 指定索引库的地址
        Directory directory = FSDirectory.open(FileSystems.getDefault().getPath(indexPath));

        // 创建索引writer
        IndexWriter writer = new IndexWriter(directory, cfg);

        // 清除以前的index
        writer.deleteAll();

        // 通过IndexWriter对象将Document写入到索引库中
        for (Document doc : docList) {
            writer.addDocument(doc);
        }

        // 关闭writer
        writer.close();
    }

    private static void searchDocument(Query query) throws IOException {
        // 1.创建DirectoryJDK 1.7以后 open只能接收Path
        Directory directory = FSDirectory.open(FileSystems.getDefault().getPath(indexPath));
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        // 通过searcher来搜索索引库,第二个参数指定需要显示的顶部记录的N条
        TopDocs topDocs = searcher.search(query, 10);

        // 根据查询条件匹配出的记录总数
        int count = topDocs.totalHits;

        System.out.println("匹配出的记录总数:" + count + "\n==========================");

        // 根据查询条件匹配出的记录
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;

        for (ScoreDoc scoreDoc : scoreDocs) {
            // 获取文档的ID
            int docId = scoreDoc.doc;
            // 通过ID获取文档
            Document doc = searcher.doc(docId);
            System.out.println("商品ID:" + doc.get("book_id"));
            System.out.println("商品名称:" + doc.get("name"));
            System.out.println("商品价格:" + doc.get("price"));
            System.out.println("商品图片地址:" + doc.get("picture"));
            System.out.println("商品描述:" + doc.get("description"));
            System.out.println("==========================");
        }
        // 关闭资源
        reader.close();

    }

    public static void indexSearch(String name) throws Exception {
        // 创建query对象
        Analyzer analyzer = new StandardAnalyzer();

        // 使用QueryParser搜索时，需要指定分词器，搜索时的分词器要和索引时的分词器一致,第一个参数：默认搜索的域的名称
        QueryParser parser = new QueryParser("description", analyzer);

        // 参数：输入的lucene的查询语句(关键字一定要大写)
        Query query = parser.parse(name);

        searchDocument(query);
    }


}
