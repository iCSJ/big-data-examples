package com.andy.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * <p> Transformation （转换/变换）算子
 *
 * @author leone
 * @since 2018-12-18
 **/
public class JavaTransformationOperation {


    private List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

    /**
     * 算子
     */
    @Test
    public void some() {

    }

    /**
     * map算子：将原来 RDD 的每个数据项通过 map 中的用户自定义函数 f 映射转变为一个新的元素，返回类型：MappedRDD
     */
    @Test
    public void map() {
        SparkConf conf = new SparkConf().setAppName("map").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // 并行化集合，初始化RDD
        JavaRDD<Integer> rdd = sparkContext.parallelize(numbers);
        // map 算子是对任何 RDD 都可以调用的，在 java 中 map 算子接受的是 function 对象
        JavaRDD<Integer> multipleNumberRDD = rdd.map((Function<Integer, Integer>) integer -> integer * 2);
        multipleNumberRDD.foreach((VoidFunction<Integer>) integer -> System.out.println(integer + ""));

        sparkContext.close();
    }

    /**
     * flatMap算子：将原来 RDD 中的每个元素通过函数 f 转换为新的元素，并将生成的 RDD 的每个集合中的元素合并为一个集合。返回类型：FlatMappedRDD
     */
    @Test
    public void flatMap() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("flatMap").setMaster("local[*]"));
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> rdd = sparkContext.parallelize(lineList);
        JavaRDD<String> words = rdd.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
        words.foreach((VoidFunction<String>) s -> System.out.println(s + ""));
        sparkContext.close();
    }


    /**
     * mapPartitions函数获取到每个分区的迭代器，在函数中通过这个分区整体的迭代器对整个分区的元素进行操作。返回类型：MapPartitionsRDD
     */
    @Test
    public void mapPartitions() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("mapPartitions").setMaster("local[*]"));
        JavaRDD<Integer> rdd = sparkContext.parallelize(numbers, 2);
        JavaRDD<Integer> result = rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) integerIterator -> {
            int isum = 0;
            while (integerIterator.hasNext()) {
                isum += integerIterator.next();
            }
            LinkedList<Integer> linkedList = new LinkedList<>();
            linkedList.add(isum);
            return linkedList.iterator();
        });

        result.foreach(e -> System.out.println(e + ""));
        sparkContext.close();
    }

    /**
     * glom 函数将每个分区形成一个数组，内部实现是返回的GlommedRDD
     */
    @Test
    public void glom() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("glom").setMaster("local[*]"));
        JavaRDD<Integer> rdd = sparkContext.parallelize(numbers, 2);
        JavaRDD<List<Integer>> glom = rdd.glom();
        glom.foreach(e -> System.out.println(e + ""));
        sparkContext.close();
    }


    /**
     * union 算子 对源RDD和参数RDD求并集后返回一个新的RDD
     */
    @Test
    public void union() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("union").setMaster("local[*]"));
        JavaRDD<Integer> rdd1 = sparkContext.parallelize(Arrays.asList(1, 2, 7, 4, 7));
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(2, 3, 3, 6, 7));

        JavaRDD<Integer> union = rdd1.union(rdd2);

        union.foreach(e -> System.out.println(e + ""));
        sparkContext.close();
    }

    /**
     * intersection 算子 对源RDD和参数RDD求交集后返回一个新的RDD
     */
    @Test
    public void intersection() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("intersection").setMaster("local[*]"));

        JavaRDD<Integer> rdd1 = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(1, 9, 3, 8, 5));

        JavaRDD<Integer> sample = rdd1.intersection(rdd2);
        System.out.println(sample.collect());

        sparkContext.close();
    }

    /**
     * join 算子(otherDataset, [numTasks])是连接操作，将输入数据集(K,V)和另外一个数据集(K,W)进行Join， 得到(K, (V,W))；该操作是对于相同K的V和W集合进行笛卡尔积 操作，也即V和W的所有组合；
     */
    @Test
    public void join() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("join").setMaster("local[*]"));
        JavaRDD<Integer> rdd1 = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        JavaPairRDD<Integer, Integer> firstRDD = rdd1.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 10));

        JavaPairRDD<Integer, Integer> secondRDD = rdd2.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 100));

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> join = firstRDD.join(secondRDD);

        join.foreach(e -> System.out.println(e + ""));

        sparkContext.close();
    }

    /**
     * leftOuterJoin 算子 根据两个RDD来进行做外连接，右边没有的值会返回一个None。右边有值的话会返回一个Some。
     */
    @Test
    public void leftOuterJoin() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("leftOuterJoin").setMaster("local[*]"));
        JavaRDD<Integer> rdd1 = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(1, 9, 3, 7, 5));

        JavaPairRDD<Integer, Integer> firstRDD = rdd1.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 10));

        JavaPairRDD<Integer, Integer> secondRDD = rdd2.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 100));

        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> result = firstRDD.leftOuterJoin(secondRDD);

        result.foreach(e -> System.out.println(e + ""));

        sparkContext.close();
    }

    /**
     * rightOuterJoin 算子
     */
    @Test
    public void rightOuterJoin() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("leftOuterJoin").setMaster("local[*]"));
        JavaRDD<Integer> rdd1 = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(Arrays.asList(1, 9, 3, 7, 5));

        JavaPairRDD<Integer, Integer> firstRDD = rdd1.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 10));

        JavaPairRDD<Integer, Integer> secondRDD = rdd2.mapToPair((PairFunction<Integer, Integer, Integer>) integer -> new Tuple2<>(integer, integer * 100));
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> result = firstRDD.rightOuterJoin(secondRDD);
        result.foreach(e -> System.out.println(e + ""));

        sparkContext.close();
    }

    /**
     * cartesian 算子 对两个RDD内的所有元素进行笛卡尔积操作。操作后，内部实现返回CartesianRDD；
     */
    @Test
    public void cartesian() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("cartesian").setMaster("local[*]"));
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(data);
        JavaPairRDD<Integer, Integer> cartesianRDD = javaRDD.cartesian(javaRDD);
        System.out.println(cartesianRDD.collect());
        sparkContext.close();
    }

    /**
     * coalesce 算子 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle
     */
    @Test
    public void coalesce() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("coalesce").setMaster("local[*]"));
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(data, 5);
        System.out.println(javaRDD.getNumPartitions());
        JavaRDD<Integer> coalesce = javaRDD.coalesce(2, true);
        System.out.println(coalesce.collect());
        System.out.println(coalesce.getNumPartitions());
        sparkContext.close();
    }


    /**
     * repartition 算子 repartition是coalesce接口中shuffle为true的简易实现，即Reshuffle RDD并随机分区，使各分区数据量尽可能平衡。若分区之后分区数远大于原分区数，则需要shuffle。
     */
    @Test
    public void repartition() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("repartition").setMaster("local[*]"));
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(data, 5);
        System.out.println(javaRDD.getNumPartitions());
        JavaRDD<Integer> coalesce = javaRDD.repartition(3);
        System.out.println(coalesce.collect());
        System.out.println(coalesce.getNumPartitions());
        sparkContext.close();
    }


    /**
     * mapPartitionsWithIndex 算子 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Iterator[T]) => Iterator[U])
     */
    @Test
    public void mapPartitionsWithIndex() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]"));
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(data, 2);
        JavaRDD<Integer> coalesce = javaRDD.mapPartitionsWithIndex((Function2<Integer, Iterator<Integer>, Iterator<Integer>>) (integer, integerIterator) -> {
            int isum = 0;
            while (integerIterator.hasNext()) {
                isum += integerIterator.next();
            }
            LinkedList<Integer> linkedList = new LinkedList<>();
            linkedList.add(isum);
            return linkedList.iterator();
        }, true);

        System.out.println(coalesce.collect());
        sparkContext.close();
    }

    /**
     * cogroup 算子:对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
     */
    @Test
    public void cogroup() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("cogroup").setMaster("local[*]"));

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "james"),
                new Tuple2<>(4, "andy")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 89),
                new Tuple2<>(3, 100),
                new Tuple2<>(2, 100),
                new Tuple2<>(4, 97)
        );

        JavaPairRDD<Integer, String> rdd1 = sparkContext.parallelizePairs(studentList);

        JavaPairRDD<Integer, Integer> rdd2 = sparkContext.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> result = rdd1.cogroup(rdd2);
        System.out.println(result.collect());

        sparkContext.close();
    }

    /**
     * filter 算子 是对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回值为 false 的元素将被过滤掉
     */
    @Test
    public void filter() {
        SparkConf sparkConf = new SparkConf().setAppName("filter").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> paraNumber = sc.parallelize(numbers);

        JavaRDD<Integer> filter = paraNumber.filter((Function<Integer, Boolean>) integer -> integer % 2 == 0);

        filter.foreach(e -> System.out.println(e + ""));

        sc.close();
    }

    /**
     * groupByKey算子 groupByKey是对每个key进行合并操作，但只生成一个sequence，groupByKey本身不能自定义操作函数。
     */
    @Test
    public void groupByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("groupByKey").setMaster("local[*]"));

        List<Tuple2<String, Integer>> scoresList = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 68),
                new Tuple2<>("class2", 85),
                new Tuple2<>("class3", 97),
                new Tuple2<>("class1", 82)
        );
        // 集合并行化
        JavaPairRDD<String, Integer> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<String, Iterable<Integer>> groupScore = score.groupByKey();

        groupScore.foreach((VoidFunction<Tuple2<String, Iterable<Integer>>>) s -> System.out.println(s + ""));

        sc.close();
    }

    /**
     * sample 算子 根据fraction指定的比例对数据进行采样，可以选择是否使用随机数进行替换，seed用于指定随机数生成器种子
     */
    @Test
    public void sample() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sample").setMaster("local[*]"));

        JavaRDD<Integer> rdd = sc.parallelize(numbers);
        JavaRDD<Integer> sample = rdd.sample(true, 2);
        System.out.println(sample.collect());

        sc.close();
    }

    /**
     * distinct 算子 对源RDD进行去重后返回一个新的RDD
     */
    @Test
    public void distinct() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("distinct").setMaster("local[*]"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 4, 5, 5, 6, 7));
        JavaRDD<Integer> distinct = rdd.distinct();
        System.out.println(distinct.collect());
        sc.close();
    }

    /**
     * aggregateByKey 算子 对源RDD进行去重后返回一个新的RDD
     */
    @Test
    public void aggregateByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("distinct").setMaster("local[*]"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 4, 5, 5, 6, 7, 8, 9));
//        JavaRDD<Integer> distinct = rdd.aggregate(0 new Function2<Integer, Integer, Integer>());
//        System.out.println(distinct.collect());
        sc.close();
    }


    /**
     * reduceByKey算子 对数据集key相同的值，都被使用指定的reduce函数聚合到一起。
     */
    @Test
    public void reduceByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("reduceByKey").setMaster("local[*]"));

        List<Tuple2<String, Integer>> scoresList = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 68),
                new Tuple2<>("class2", 85),
                new Tuple2<>("class3", 97),
                new Tuple2<>("class1", 82)
        );
        // 集合并行化
        JavaPairRDD<String, Integer> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<String, Integer> pairRDD = score.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) t -> System.out.println(t._1 + " === sum: " + t._2));

        sc.close();
    }


    /**
     * sortByKey 算子：排序
     */
    public void sortByKey() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sortByKey").setMaster("local[*]"));
        List<Tuple2<Integer, String>> scoresList = Arrays.asList(
                new Tuple2<>(90, "tom"),
                new Tuple2<>(68, "jack"),
                new Tuple2<>(85, "james"),
                new Tuple2<>(82, "andy")
        );
        // 集合并行化
        JavaPairRDD<Integer, String> score = sc.parallelizePairs(scoresList);

        JavaPairRDD<Integer, String> pairRDD = score.sortByKey(false);

        pairRDD.foreach((VoidFunction<Tuple2<Integer, String>>) t -> System.out.println(t._1 + ":" + t._2));

        sc.close();
    }

    /**
     * pipe 算子 通过一个shell命令来对RDD各分区进行“管道化”。通过pipe变换将一些shell命令用于Spark中生成的新RDD
     */
    @Test
    public void pipe() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("pipe").setMaster("local[*]"));
        List<String> data = Arrays.asList("hi", "hello", "how", "are", "you");
        sc.parallelize(data)
                .pipe("e:\\b.bat")
                .collect()
                .forEach(System.out::println);
        sc.close();
    }

    /**
     * join 算子：排序
     */
    @Test
    public void join2() {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("join").setMaster("local[*]"));

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "james"),
                new Tuple2<>(4, "andy")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 89),
                new Tuple2<>(4, 97)
        );

        // 并行化两个rdd
        JavaPairRDD<Integer, String> student = sc.parallelizePairs(studentList);

        JavaPairRDD<Integer, Integer> score = sc.parallelizePairs(scoreList);

        JavaPairRDD join = student.join(score);

        join.foreach((VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>) t -> System.out.println("id:" + t._1 + "\t\tname:" + t._2._1 + "\t\tscore:" + t._2._2));

        sc.close();
    }


}
