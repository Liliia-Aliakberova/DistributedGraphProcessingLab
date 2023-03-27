package exercise_3;

import scala.Tuple2;

import java.util.ArrayList;

public class Node extends Tuple2<Integer, ArrayList<String>> {
    public Node(Integer _1, ArrayList<String> _2) {
        super(_1, _2);
    }
    public Node(Integer _1) {
        super(_1, new ArrayList<String>());
    }
    public Node() {
        super(0, new ArrayList<String>());
    }

}
