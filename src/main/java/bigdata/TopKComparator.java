package bigdata;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class TopKComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
        return o1._2.compareTo(o2._2);
    }
}
