package runner;

import control.Query2_functions;
import control.Query3_functions;
import control.SparkWorker;
import control.TimeClass;
import entities.SorterClass;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;


public class StructureHouses {

    private static String OUTPUT_DIRECTORY;
    private static String INPUT_DIRECTORY;

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if (args.length != 2) {
            System.out.println("Wrong number of arguments");
            return;
        }

        OUTPUT_DIRECTORY = args[1];
        INPUT_DIRECTORY = args[0];

        SparkWorker.getInstance().initSparkContext("SABDanielloIonita", "local[*]");

        query1();
        query2();
        query3();

        SparkWorker.getInstance().closeConnection();
    }

    private static void query1() {
        TimeClass.getInstance().start();

        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);
        data
                .filter(x -> x.isProperty() == 1)               //getting only tuples with instant values
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getHouseid(), x.getTimestamp()), x.getValue()))
                .reduceByKey((x, y) -> x + y)                   //sum of the plugs with the same timestamp and house_id
                .filter(x -> x._2 >= 350)                       //filter by instant value greater then 350
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))    //grouping by house_id
                .reduceByKey(Math::max)
                .saveAsTextFile(OUTPUT_DIRECTORY + "/query1output");

        TimeClass.getInstance().stop();

    }

    private static void query2() {
        TimeClass.getInstance().start();
        Query2_functions q2 = Query2_functions.getInstance();

        //parsing input tuples in SorterClass objects
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        //getting only tuples related to total power consumption
        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);

        //getting the values related to the starting power consumption for each
        //  house_id
        //  timezone
        //  day
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsStarterValue = q2.q2_getPlugsMinTimestampValue(dataFiltered);

        //getting the values related to the final power consumption for each
        //  house_id
        //  timezone
        //  day
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsFinalValues = q2.q2_getPlugsMaxTimestampValue(dataFiltered);

        //computing the difference between final and starter values in order to find the consumption for each
        //  house_id
        //  timezone
        //  day
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> dailyValues = q2.q2_getDailyValue(plugsStarterValue, plugsFinalValues);

        //computing the average
        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageThroughDays = q2.q2_computeAverage(dailyValues);

        //computing the standard deviation
        JavaPairRDD<Tuple2<Integer, Integer>, Double> standardDeviation = q2.q2_computeStandardDeviation(dailyValues, averageThroughDays);

        averageThroughDays.saveAsTextFile(OUTPUT_DIRECTORY + "/query2mean");
        standardDeviation.saveAsTextFile(OUTPUT_DIRECTORY + "/query2standardDeviation");

        TimeClass.getInstance().stop();
    }

    private static void query3() {

        TimeClass.getInstance().start();

        Query3_functions q3 = Query3_functions.getInstance();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);   //dataset parsing from input directory

        //getting only values related to the total power consumption
        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);

        //getting the starter value for each plug during different days and timezones
        JavaPairRDD<Tuple5
                <
                Integer/*house_id*/,
                Integer/*plug_id*/,
                Integer/*timezone*/,
                Integer/*day*/,
                Integer/*daytype*/
                >,
                Double> plugsStarterValue = q3.q3_getPlugStarterValue(dataFiltered);

        //getting the final value for each plug during different days and timezones
        JavaPairRDD<Tuple5
                <
                Integer/*house_id*/,
                Integer/*plug_id*/,
                Integer/*timezone*/,
                Integer/*day*/,
                Integer/*daytype*/
                >,
                Double> plugsFinalValues = q3.q3_getPlugFinalValue(dataFiltered);

        //computing the total power consumption per-plug during different days and timezones
        JavaPairRDD<Tuple5
                <
                Integer/*house_id*/,
                Integer/*plug_id*/,
                Integer/*timezone*/,
                Integer/*day*/,
                Integer/*daytype*/
                >, Double> dailyvalue = q3.q3_getDailyValue(plugsStarterValue, plugsFinalValues);

        //computing the average of total consumption through days during the high-end frame
        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageTopTime = q3.q3_getAverageForTimeFrame(1, dailyvalue);

        //computing the average of total consumption through days during the low-end frame
        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageDownTime = q3.q3_getAverageForTimeFrame(0, dailyvalue);

        //getting the difference between high-end and low-end time slots and sorting in descending order
        JavaPairRDD<Tuple2<Integer, Integer>, Double> sorting = q3.q3_sortData(averageDownTime, averageTopTime);

        sorting.saveAsTextFile(OUTPUT_DIRECTORY + "/query3");

        TimeClass.getInstance().stop();


    }
}
