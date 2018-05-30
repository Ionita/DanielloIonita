package runner;

import control.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import entities.SorterClass;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;


public class StructureHouses_test {

    private static String OUTPUT_DIRECTORY;
    private static String INPUT_DIRECTORY;

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if(args.length != 2){
            System.out.println("wrong number of arguments");
            return;
        }
        OUTPUT_DIRECTORY = args[1];
        INPUT_DIRECTORY = args[0];


        createFileIfItExists(OUTPUT_DIRECTORY + "/query1results.csv");
        createFileIfItExists(OUTPUT_DIRECTORY + "/query2results.csv");
        createFileIfItExists(OUTPUT_DIRECTORY + "/query3results.csv");

        SparkWorker.getInstance().initSparkContext("SABD", "local[*]");

        for(int i = 0; i < 1; i++) {
            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query1output");
            query1();
        }
//        for(int i = 0; i < 1; i++) {
//            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query2mean");
//            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query2standardDeviation");
//            query2();
//        }
//        for(int i = 0; i < 1; i++) {
//            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query3");
//            query3();
//        }

        SparkWorker.getInstance().closeConnection();
    }

    private static void query1() {
        TimeClass.getInstance().start();

        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        data
                .filter(x -> x.isProperty() == 1)        //getting only tuples with instant values
                .mapToPair(x -> new Tuple2<>(x.getHouseid(), x.getValue()))
                .reduceByKey((x, y) -> x+y)

                .foreach(x -> {
                    if(x._1 == 5)
                    System.out.println(x);
                });

        data
                .filter(x -> x.isProperty() == 1)        //getting only tuples with instant values
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getHouseid(), x.getTimestamp()), x.getValue()))
                .reduceByKey((x, y) -> x + y)              //sum of the plugs with the same timestamp and house_id
                .filter(x -> x._2 >= 350)               //filter by instant value greater then 350
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))              //grouping by house_id
                .reduceByKey((x, y) -> {
                    if (x>y){
                        System.out.println(x);
                        return x;
                    }
                    else
                        return y;
                })
                .saveAsTextFile(OUTPUT_DIRECTORY + "/query1output");

        TimeClass.getInstance().stop();

    }

    private static void query2() {
        TimeClass.getInstance().start();
        Query2_functions q2 = Query2_functions.getInstance();

        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);  //taking total value energy

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsStarterValue = q2.q2_getPlugsMinTimestampValue(dataFiltered);
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsFinalValues = q2.q2_getPlugsMaxTimestampValue(dataFiltered);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> dailyValues = q2.q2_getDailyValue(plugsStarterValue, plugsFinalValues);

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageThroughDays = q2.q2_computeAverage(dailyValues);
        JavaPairRDD<Tuple2<Integer, Integer>, Double> standardDeviation = q2.q2_computeStandardDeviation(dailyValues, averageThroughDays);

        averageThroughDays.saveAsTextFile(OUTPUT_DIRECTORY + "/query2mean");
        standardDeviation.saveAsTextFile(OUTPUT_DIRECTORY + "/query2standardDeviation");

        TimeClass.getInstance().stop();
    }

    private static void query3() {

        TimeClass.getInstance().start();

        Query3_functions q3 = Query3_functions.getInstance();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);   //dataset parsing from input directory

        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);  //taking total value energy

        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >,
                Double> plugsStarterValue = q3.q3_getPlugStarterValue(dataFiltered);


        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >,
                Double> plugsFinalValues = q3.q3_getPlugFinalValue(dataFiltered);


        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >, Double> dailyvalue = q3.q3_getDailyValue(plugsStarterValue, plugsFinalValues);


        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageTopTime = q3.q3_getAverageForTimeFrame(1, dailyvalue);

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageDownTime = q3.q3_getAverageForTimeFrame(0, dailyvalue);

        JavaPairRDD<Tuple2<Integer, Integer>, Double> sorting = q3.q3_sortData(averageDownTime, averageTopTime);

        sorting.saveAsTextFile(OUTPUT_DIRECTORY + "/query3");

        TimeClass.getInstance().stop();


    }

    private static Boolean deleteFileIfItExists(String filepath){
        File directory = new File(filepath);
        if(directory.exists()){
            String[] entries = directory.list();
            assert entries != null;
            for(String s: entries){
                File currentFile = new File(directory.getPath(),s);
                if(!currentFile.delete())
                    return false;
            }
            return !directory.delete();
        }
        return true;
    }

    private static Boolean createFileIfItExists(String filepath){
        File directory = new File(filepath);
        if(!directory.exists()) {
            try {
                return directory.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}
