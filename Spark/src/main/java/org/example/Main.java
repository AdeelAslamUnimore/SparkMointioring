package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public class Main {




    private static final Pattern SPACE = Pattern.compile(" ");

    private static final int numEdges = 200;
    private static final int numVertices = 100;
    private static final Random rand = new Random(42);

    static List<Tuple2<Integer, Integer>> generateGraph() {
        Set<Tuple2<Integer, Integer>> edges = new HashSet<>(numEdges);
        while (edges.size() < numEdges) {
            int from = rand.nextInt(numVertices);
            int to = rand.nextInt(numVertices);
            Tuple2<Integer, Integer> e = new Tuple2<>(from, to);
            if (from != to) {
                edges.add(e);
            }
        }
        return new ArrayList<>(edges);
    }

    static class ProjectFn implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>,
                Integer, Integer> {
        static final ProjectFn INSTANCE = new ProjectFn();

        @Override
        public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
            return new Tuple2<>(triple._2()._2(), triple._2()._1());
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .config("spark.extraListeners","org.example.Model")
                .appName("JavaTC")
                .getOrCreate();
        spark.sparkContext().addSparkListener(new Model());
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = (args.length > 0) ? Integer.parseInt(args[0]): 2;
        JavaPairRDD<Integer, Integer> tc = jsc.parallelizePairs(generateGraph(), slices).cache();

        // Linear transitive closure: each round grows paths by one edge,
        // by joining the graph's edges with the already-discovered paths.
        // e.g. join the path (y, z) from the TC with the edge (x, y) from
        // the graph to obtain the path (x, z).

        // Because join() joins on keys, the edges are stored in reversed order.
        JavaPairRDD<Integer, Integer> edges = tc.mapToPair(e -> new Tuple2<>(e._2(), e._1()));

        long oldCount;
        long nextCount = tc.count();
        do {
            oldCount = nextCount;
            // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
            // then project the result to obtain the new (x, z) paths.
            tc = tc.union(tc.join(edges).mapToPair(ProjectFn.INSTANCE)).distinct().cache();
            nextCount = tc.count();
        } while (nextCount != oldCount);

        System.out.println("TC has " + tc.count() + " edges.");
        spark.stop();
    }
}
   // public static void main(String[] args) {





        //SparkSession spark = SparkSession .builder()
        //       .appName("JavaWordCount") .config("spark.master", "local").getOrCreate();
        // spark.sparkContext().addSparkListener(new SparkListener());
        // RDD rdd = spark.sparkContext().textFile("/home/adeel/Desktop/File.txt",1);

        //System.out.println("0000000000000000000"+rdd.count());

        //

        //  System.out.println("The spark configuration is "+spark.sparkContext().appName());
        //System.exit(-1);


   // }