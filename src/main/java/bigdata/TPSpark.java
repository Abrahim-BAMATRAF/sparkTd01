package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.util.List;

public class TPSpark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TP Spark");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> textFile = context.textFile(args[0]);
        System.out.println(textFile.getNumPartitions());

        // Filtering
        JavaRDD<String> filteredTextFile = textFile.filter(x -> {
            String[] line = x.split(",");
            if (line[0].equals("Country")) {
                return false;
            }
            try {
                int pop = Integer.parseInt(line[4]);
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        System.out.println("before filtering : " + textFile.count());

        System.out.println("after filtering : " + filteredTextFile.count());

        //-------------------------RDD of "city, pop" ---------------------------

        JavaRDD<String> cityPop = filteredTextFile.map(x -> {
            String[] line = x.split(",");
            String cityName = line[1];
            String pop = line[4];
            return cityName + "," + pop;
        });

        System.out.println("city pop count: "+cityPop.count());

        //--------------------------RDD of (city, pop)---------------------------

        JavaPairRDD<String, Integer> cityPopPairs = cityPop.mapToPair(s -> {
            String[] pairList = s.split(",");
            return new Tuple2(pairList[0], Integer.parseInt(pairList[1]));
        });

        System.out.println("city pop pairs count: " + cityPopPairs.count());

        int limit = 50;
        int index = 0;
        for (Tuple2 pair : cityPopPairs.collect()) {
            System.out.println("City : " + pair._1 +" , Population : " + pair._2);
            index++;
            if(index >= limit) break;
        }

        //------------------RDD of pop ------------------------------------------

        JavaDoubleRDD populations = cityPopPairs.mapToDouble(pair -> pair._2);

        StatCounter stats = populations.stats();
        System.out.println("the stats of all pop : " + stats);

        //-------------------Histogram  (#TODO use samples to better distribute the buckets)---------------------

        // Rice rule to calculate the bin number
        int number = (int) (2 * Math.pow(stats.count(), 1.0/3.0));
        System.out.println("The number of buckets : " + number);

        Tuple2<double[], long[]> histogram = populations.histogram(number);

        System.out.println("The histogram : ");
        index = 0;
        for (long x : histogram._2){
            System.out.println("The bucket " + index++ + " has : " + x + " elements.");
        }

        //-----------------Top K cities---------------------------------

        List<Tuple2<String, Integer>> topKList =  cityPopPairs.top(10, new TopKComparator());

        System.out.println("---------------------------------------");
        System.out.println("Printing the topK elements");
        for (Tuple2 pair : topKList) {
            System.out.println("City : " + pair._1 +" , Population : " + pair._2);
        }

        //--------------------------------------------------------
    }
}