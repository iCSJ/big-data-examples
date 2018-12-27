package com.andy.lucene;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since: 2018-11-23
 **/
public class BookMain {

    private static final Logger logger = LoggerFactory.getLogger(BookMain.class);

    private static List<Book> books = new ArrayList<>();

    private static String docPath = "E:\\tmp\\lucene\\docDir";
    private static String indexPath = "E:\\tmp\\lucene\\indexDir";

    static {
        books.add(new Book(1, "Java核心编程思想", 23, "http://www.taobao.com/image.jpg", "Java是一门面向对象编程语言，不仅吸收了C++语言的各种优点，还摒弃了C++里难以理解的多继承、指针等概念，因此Java语言具有功能强大和简单易用两个特征。Java语言作为静态面向对象编程语言的代表，极好地实现了面向对象理论，允许程序员以优雅的思维方式进行复杂的编程"));
        books.add(new Book(2, "mysql数据库设计", 47, "http://www.taobao.com/image.jpg", "MySQL是一个关系型数据库管理系统，由瑞典MySQL AB 公司开发，目前属于 Oracle 旗下产品。MySQL 是最流行的关系型数据库管理系统之一，在 WEB 应用方面，MySQL是最好的 RDBMS (Relational Database Management System，关系数据库管理系统) 应用软件。"));
        books.add(new Book(3, "JavaWeb进阶", 75, "http://www.jd.com/image.jpg", "Java Web，是用Java技术来解决相关web互联网领域的技术总和。web包括：web服务器和web客户端两部分。Java在客户端的应用有java applet，不过使用得很少，Java在服务器端的应用非常的丰富，比如Servlet，JSP和第三方框架等等。Java技术对Web领域的发展注入了强大的动力。"));
        books.add(new Book(4, "spring入门", 38, "http://www.tianmao.com/image.jpg", "Spring是一个开放源代码的设计层面框架，他解决的是业务逻辑层和其他各层的松耦合问题，因此它将面向接口的编程思想贯穿整个系统应用。Spring是于2003 年兴起的一个轻量级的Java 开发框架，由Rod Johnson创建。简单来说，Spring是一个分层的JavaSE/EE full-stack(一站式) 轻量级开源框架"));
        books.add(new Book(5, "springCloud微服务实战", 42, "http://www.yamaxun.com/image.jpg", "Spring Cloud是一系列框架的有序集合。它利用Spring Boot的开发便利性巧妙地简化了分布式系统基础设施的开发，如服务发现注册、配置中心、消息总线、负载均衡、断路器、数据监控等，都可以用Spring Boot的开发风格做到一键启动和部署。Spring Cloud并没有重复制造轮子，它只是将目前各家公司开发的比较成熟、经得起实际考验的服务框架组合起来，通过Spring Boot风格进行再封装屏蔽掉了复杂的配置和实现原理，最终给开发者留出了一套简单易懂、易部署和易维护的分布式系统开发工具包。"));
        books.add(new Book(5, "JavaScript高级", 71, "http://www.tianmao.com/image.jpg", "JavaScript 是属于网络的脚本语言 JavaScript 被数百万计的网页用来改进设计、验证表单、检测浏览器、创建cookies，以及更多的应用。"));
    }

    public static void main(String[] args) throws Exception {

        Map<String, String> doc = new HashMap<>();
        doc.put("文件名称", "fileName");
        doc.put("文件大小", "fileSize");
        doc.put("文件路径", "filePath");
        doc.put("文件内容", "fileContent");

        Map<String, String> book = new HashMap<>();
        book.put("商品ID", "book_id");
        book.put("商品名称", "name");
        book.put("商品价格", "price");
        book.put("商品图片地址", "picture");
        book.put("商品描述", "description");

        long start = System.currentTimeMillis();
//        createIndex();
//        createFileIndex();
        indexSearch("fileName:git", doc);
//        indexSearch("description:spring");


        logger.info("一共花费了:{}毫秒!", (System.currentTimeMillis() - start));
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

    /**
     * 从文件创建索引
     */
    private static void createFileIndex() throws Exception {
        Path path = new File(indexPath).toPath();
        Directory directory = FSDirectory.open(path);
        //标准分词器
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        // 清除以前的index
        writer.deleteAll();

        //需要分析的文件目录 ,注意目录下不能用文件夹
        File[] listFiles = new File(docPath).listFiles();
        if (null != listFiles && listFiles.length > 0) {
            for (File file : listFiles) {
                Document document = new Document();
                //文件名称
                Field fileName = new TextField("fileName", file.getName(), Field.Store.YES);
                //文件大小
                Field fileSize = new StringField("fileSize", Long.toString(FileUtils.sizeOf(file)), Field.Store.YES);
                //文件路径
                Field filePath = new StoredField("filePath", file.getPath());
                //文件内容
                Field fileContent = new TextField("fileContent", FileUtils.readFileToString(file, StandardCharsets.UTF_8), Field.Store.YES);
                document.add(fileName);
                document.add(fileSize);
                document.add(filePath);
                document.add(fileContent);
                writer.addDocument(document);
                logger.info("索引文件:{}", file.getPath());
            }
        }
        writer.close();
    }


    /**
     * @param query
     * @throws IOException
     */
    private static void searchDocument(Query query, Map<String, String> map) throws IOException {
        // 1.创建DirectoryJDK 1.7以后 open只能接收Path
        Directory directory = FSDirectory.open(FileSystems.getDefault().getPath(indexPath));
        IndexReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        // 通过searcher来搜索索引库,第二个参数指定需要显示的顶部记录的N条
        TopDocs topDocs = searcher.search(query, 10);

        // 根据查询条件匹配出的记录总数
        int count = topDocs.totalHits;

        System.out.println("匹配出的记录总数:[ " + count + " ]\n==========================");
        // 根据查询条件匹配出的记录
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            // 获取文档的ID
            int docId = scoreDoc.doc;
            // 通过ID获取文档
            Document doc = searcher.doc(docId);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                System.out.println(entry.getKey() + ":" + doc.get(entry.getValue()));
            }
            System.out.println("==========================");
        }
        // 关闭资源
        reader.close();

    }

    public static void indexSearch(String name, Map<String, String> map) throws Exception {
        // 创建query对象
        Analyzer analyzer = new StandardAnalyzer();
        // 使用QueryParser搜索时，需要指定分词器，搜索时的分词器要和索引时的分词器一致,第一个参数：默认搜索的域的名称
        QueryParser parser = new QueryParser("description", analyzer);
        // 参数：输入的lucene的查询语句(关键字一定要大写)
        Query query = parser.parse(name);
        searchDocument(query, map);
    }


}
