package exercise_2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_2 {
    static final Integer MVALUE = Integer.MAX_VALUE;

    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {

            // Implementing the first superstep
            if (message.equals(MVALUE)) {
                return vertexValue;
            } else {
                // Implementing futher superstep
                return Math.min(vertexValue, message);
            }
        }}

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Long sourceId = triplet.srcId();
            Long destinationId = triplet.dstId();
            Integer cost = triplet.attr();
            Integer sourceVertex = triplet.srcAttr();
            Integer destinationVertex = triplet.dstAttr();

            if (sourceVertex.equals(MVALUE)) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Integer>>().iterator()).asScala();
            } else {
                Integer sentValue = sourceVertex + cost;
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Integer>(triplet.dstId(), sentValue)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return null;
        }
    }

    public static void shortestPaths(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Integer>> vertices = Lists.newArrayList(
                new Tuple2<Object, Integer>(1l, 0),
                new Tuple2<Object, Integer>(2l, Integer.MAX_VALUE),
                new Tuple2<Object, Integer>(3l, Integer.MAX_VALUE),
                new Tuple2<Object, Integer>(4l, Integer.MAX_VALUE),
                new Tuple2<Object, Integer>(5l, Integer.MAX_VALUE),
                new Tuple2<Object, Integer>(6l, Integer.MAX_VALUE)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), 1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(MVALUE,
                        MVALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .toJavaRDD()
                // Adding the sorting to match the output
                .sortBy(n -> ((Tuple2<Object, Integer>) n)._1,true,0)
                .foreach(v -> {
                    Tuple2<Object, Integer> vertex = (Tuple2<Object, Integer>) v;
                    System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is " + vertex._2);
                });
    }}
