package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {


    static final Integer MVALUE = Integer.MAX_VALUE;
    static final Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();


    private static class VProg extends AbstractFunction3<Long,Node,Node,Node> implements Serializable {
        @Override
        public Node apply(Long nodeID, Node nodeValue, Node message) {
            System.out.println("[ VProg ] nodeID: '" +  nodeID +  "' nodeValue: '" +  nodeValue + "' message: '" + message + "'" );

            if (message._1.equals(MVALUE)) {
                System.out.println("[ VProg ] State 0 -> nodeID: '" +  nodeID +  "'");
                return nodeValue;
            } else {
                System.out.println("[ VProg ] nodeID: '" +  nodeID +  "' '" + Math.min(nodeValue._1, message._1) + "' value");
                if(nodeValue._1<=message._1){
                    return nodeValue;
                }
                return message;
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Node,Integer>, Iterator<Tuple2<Object,Node>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Node>> apply(EdgeTriplet<Node, Integer> triplet) {

            Long srcId = triplet.srcId();
            Long dstId = triplet.dstId();
            Integer weight = triplet.attr();
            Node sourceNode = triplet.srcAttr();
            Node destinationNode = triplet.dstAttr();

            if ( sourceNode._1.equals(MVALUE) ) {
                System.out.println("[ sendMsg ] srcId: '" +  srcId +  " [" + sourceNode + "]'  to dstId: '" + dstId + " [" + destinationNode + "]'");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Node>>().iterator()).asScala();
            } else {
                Integer minpath = sourceNode._1+weight;
                ArrayList<String> path =sourceNode._2;
                path.add(labels.get(srcId));
                Node nodemessage= new Node( minpath,path);

                System.out.println("[ sendMsg ] srcId: '" +  srcId +  " [" + sourceNode + "]' send '" + minpath + "' to dstId: '" + dstId + " [" + destinationNode + "]'");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Node>(triplet.dstId(), nodemessage)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Node,Node,Node> implements Serializable {
        @Override
        public Node apply(Node msg1, Node msg2) {
            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        List<Tuple2<Object,Node>> vertices = Lists.newArrayList(
                new Tuple2<Object,Node>(1l, new Node(0)),
                new Tuple2<Object,Node>(2l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(2l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(2l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(3l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(4l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(5l, new Node(Integer.MAX_VALUE)),
                new Tuple2<Object,Node>(6l, new Node(Integer.MAX_VALUE))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Node>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Node,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Node(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Node.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Node.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Node(Integer.MAX_VALUE),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Node.class))
                .vertices()
                .toJavaRDD()
                .sortBy(node -> ((Tuple2<Object, Node>) node)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Node> act = (Tuple2<Object,Node>)v;
                    Node node = (Node) act._2;
                    ArrayList<String> path =node._2;
                    path.add(labels.get((Long) act._1));

                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(act._1)+" is "+path +" with cost "+node._1  );
                });
    }
	
}
