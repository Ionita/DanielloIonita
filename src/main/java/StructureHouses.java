import control.SparkWorker;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import javax.xml.crypto.Data;


public class StructureHouses {

    public static void main(String[] args) {
        groupByHouse_id();
    }
    private static void groupByHouse_id() {
        SparkWorker.getInstance().initSparkContext("simple sorter", "local[*]");
        JavaRDD<main.java.entities.SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        JavaPairRDD<Integer, Double> newDataset = data.filter(x-> x.isProperty() == 1 && x.getValue() >= 350).mapToPair(x->new Tuple2<>(x.getHouseid(), x.getValue()));
        newDataset = newDataset.reduceByKey((x,y) -> x);
        newDataset.foreach(x-> System.out.printf("housdeid: " + x._1 + "\nvalue: " + x._2 ));
    }

}
