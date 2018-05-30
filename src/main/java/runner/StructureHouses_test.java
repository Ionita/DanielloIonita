package runner;

import control.SparkWorker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import control.TimeClass_test;
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

        for(int i = 0; i < 50; i++) {
            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query1output");
            query1();
        }
        for(int i = 0; i < 50; i++) {
            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query2mean");
            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query2standardDeviation");
            query2();
        }
        for(int i = 0; i < 50; i++) {
            deleteFileIfItExists(OUTPUT_DIRECTORY + "/query3");
            query3();
        }

        SparkWorker.getInstance().closeConnection();
    }

    private static void query1(){
        TimeClass_test.getInstance().start();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);
        data
                .filter(x-> x.isProperty() == 1)    //getting only tuples with instant values
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getHouseid(), x.getTimestamp()), x.getValue()))
                .reduceByKey((x,y) -> x+y)          //sum of the plugs with the same timestamp and house_id
                .filter(x -> x._2 >= 350)           //filter by instant value greater then 350
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))              //grouping by house_id
                .reduceByKey(Math::max)
                .saveAsTextFile(OUTPUT_DIRECTORY + "/query1output");

        TimeClass_test.getInstance().stop(OUTPUT_DIRECTORY + "/query1results.csv");

    }

    private static void query2(){
        TimeClass_test.getInstance().start();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsStarterValue = dataFiltered
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2() < y._2())
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsFinalValues = dataFiltered
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2() > y._2())
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> dailyvalue =
                plugsFinalValues.join(plugsStarterValue)
                        .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), Math.max(x._2._1 - x._2._2, 0.0)));
//                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), x._2._1 - x._2._2));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageThroughDays=
                dailyvalue
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1/x._2._2));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> standardDeviation =
                dailyvalue
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, x._1._3())))
                        .join(averageThroughDays)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), new Tuple2<>(Math.pow(x._2._1._1 - x._2._2, 2),  1.0)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), Math.sqrt(x._2._1/(x._2._2-1.0))));

        averageThroughDays.saveAsTextFile(OUTPUT_DIRECTORY + "/query2mean");
        standardDeviation.saveAsTextFile(OUTPUT_DIRECTORY + "/query2standardDeviation");

        TimeClass_test.getInstance().stop(OUTPUT_DIRECTORY + "/query2results.csv");
    }

    private static void query3(){


        TimeClass_test.getInstance().start();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);

        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >,
                Double> plugsStarterValue =
                dataFiltered
                        .mapToPair(StructureHouses_test::calculateDay)
                        .reduceByKey((x, y) -> {
                            if(x._2() < y._2())
                                return x;
                            else
                                return y;
                        })
                        .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1()));


        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >,
                Double> plugsFinalValues =
                dataFiltered
                        .mapToPair(StructureHouses_test::calculateDay)
                        .reduceByKey((x, y) -> {
                            if(x._2() > y._2()) {
                                return x;
                            }
                            else {
                                return y;
                            }
                        })
                        .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1()));

        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >, Double> dailyvalue =
                plugsFinalValues.join(plugsStarterValue)
                        .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), Math.max(x._2._1 - x._2._2, 0.0)));

        //dailyvalue.foreach(x -> System.out.println(x._1));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageTopTime =
                dailyvalue
                        .filter(x -> x._1._5() == 1)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1()/x._2._2()));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageDownTime =
                dailyvalue
                        .filter(x -> x._1._5() == 0)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), x._2._1/x._2._2));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> sorting =
                averageTopTime
                        .join(averageDownTime)
                        .mapToPair(x -> new Tuple2<>(x._2._1 - x._2._2, x._1)) //value first
                        .sortByKey()
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._2._1, x._2._2), x._1));

        sorting.saveAsTextFile(OUTPUT_DIRECTORY + "/query3");

        TimeClass_test.getInstance().stop(OUTPUT_DIRECTORY + "/query3results.csv");



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

    private static Tuple2<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Double, Integer>> calculateDay(SorterClass x) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(x.getTimestamp() * 1000);
        int day_of_week = c.get(Calendar.DAY_OF_WEEK);
        int day_of_month = c.get(Calendar.DAY_OF_MONTH);
        int month = c.get(Calendar.MONTH);
        int year = c.get(Calendar.YEAR);
        if (x.getTimezone() == 0 ||
                x.getTimezone() == 3 ||
                day_of_week == 1 ||
                day_of_week == 7) //fascia bassa
            return new Tuple2<>(new Tuple5<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay(), 0), new Tuple2<>(x.getValue(), x.getTimestamp()));
        else
            return new Tuple2<>(new Tuple5<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay(), 1), new Tuple2<>(x.getValue(), x.getTimestamp()));
    }
}
