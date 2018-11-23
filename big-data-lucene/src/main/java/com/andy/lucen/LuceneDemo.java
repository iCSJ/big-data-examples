package com.andy.lucen;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * <p>
 *
 * @author: Leone
 * @since: 2018-11-23
 **/
public class LuceneDemo {

    public static void main(String[] args) throws Exception {
        String dirPath = "D:\\lucene\\docDir";
        String indexPath = "D:\\lucene\\indexDir";
        createIndex(dirPath, indexPath, false);
    }

    private static void createIndex(String dirPath, String indexPath, boolean flag) throws IOException {
        long startTime = System.currentTimeMillis();

        Directory directory = FSDirectory.open(Paths.get(indexPath));

        Path docDirPath = Paths.get(dirPath);
        // 分词器
        Analyzer analyzer = new StandardAnalyzer();

        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
        if (flag) {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        } else {
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        }
        IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
        indexDocs(indexWriter, docDirPath);
        indexWriter.close();
        System.out.println(System.currentTimeMillis() - startTime);

    }


    /**
     * 根据文件路径对文件内容进行索引
     * 如果是目录则扫描目录下的文件
     *
     * @param indexWriter
     * @param path
     * @throws IOException
     */
    public static void indexDocs(final IndexWriter indexWriter, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            //目录
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    indexDoc(indexWriter, file, attrs.lastModifiedTime().toMillis());
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(indexWriter, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    /**
     * 根据文件路径对文件内容进行索引
     *
     * @param indexWriter
     * @param path
     * @throws IOException
     */
    public static void indexDoc(IndexWriter indexWriter, Path path, long lastModified) throws IOException {
//        Document document = new Document();
//        Field pathField = new StringField("path", path.toString(), Field.Store.YES);
//        document.add(pathField);
//        Field lastModifiedField = new LongField("modified", lastModified, Field.Store.NO);
//        document.add(lastModifiedField);
//        Field contentField = new TextField("content", new BufferedReader(
//                new InputStreamReader(Files.newInputStream(path, new OpenOption[0]), StandardCharsets.UTF_8)
//        ));
//        document.add(contentField);
//        if (indexWriter.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
//            indexWriter.addDocument(document);
//        } else {
//            indexWriter.updateDocument(new Term("path", path.toString()), document);
//        }
//        indexWriter.commit();
    }

}

