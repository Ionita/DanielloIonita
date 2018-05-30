package runner;

import control.SparkWorker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import control.TimeClass;
import entities.SorterClass;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import java.util.Calendar;


public class StructureHouses {

    private static String OUTPUT_DIRECTORY;     //directory where results are saved
    private static String INPUT_DIRECTORY;      //directory where dataset is

    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        if(args.length != 2){
            System.out.println("Wrong number of arguments");
            return;
        }

        OUTPUT_DIRECTORY = args[1];
        INPUT_DIRECTORY = args[0];

        SparkWorker.getInstance().initSparkContext("SABDanielloIonita", "local");

        query1();       //query 1 computation
        query2();       //query 2 computation
        query3();       //query 3 computation

        SparkWorker.getInstance().closeConnection();
    }

    private static void query1(){
        TimeClass.getInstance().start();

        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);
        data
                .filter(x-> x.isProperty() == 1)        //getting only tuples with instant values
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.getHouseid(), x.getTimestamp()), x.getValue()))
                .reduceByKey((x,y) -> x+y)              //sum of the plugs with the same timestamp and house_id
                .filter(x -> x._2 >= 350)               //filter by instant value greater then 350
                .mapToPair(x -> new Tuple2<>(x._1._1, x._2))              //grouping by house_id
                .reduceByKey(Math::max)
                .saveAsTextFile(OUTPUT_DIRECTORY + "/query1output");

        TimeClass.getInstance().stop();

    }

    private static void query2(){
        TimeClass.getInstance().start();
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile(INPUT_DIRECTORY);

        JavaRDD<SorterClass> dataFiltered = data.filter(x -> x.isProperty() == 0);  //taking total value energy

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsMinValues = dataFiltered
                        .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                        .reduceByKey((x, y) -> {
                            if(Math.min(x._1, y._1) == x._1)     //timestamp order control
                                return x;
                            else
                                return y;
                        })
                        .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                        .reduceByKey((x , y) -> x+y);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsMaxValues = dataFiltered
                    .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                    .reduceByKey((x, y) -> {
                        if(Math.max(x._1, y._1) == x._1)
                            return x;
                        else
                            return y;
                    })
                    .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                    .reduceByKey((x , y) -> x+y);


        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsStarterValue = dataFiltered
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2 < y._2)     //timestamp order control
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);

        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> plugsFinalValues = dataFiltered
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2 > y._2)
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);


        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> dailyTmpvalue =
                plugsFinalValues.join(plugsStarterValue)
                        .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), x._2._1 - x._2._2));




        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> dailyvalue =
                plugsMinValues.join(plugsMaxValues)
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), x._2._2 - x._2._1));
//                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), x._2._1 - x._2._2));


        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> checkValues =
                dailyvalue
                        .join(dailyTmpvalue)
                .mapToPair(x -> {
                    if(x._2()._1.equals(x._2()._2))
                        return new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(),x._1._3()), x._2._1);
                    else
                        return new Tuple2<>(new Tuple3<>(0, 0, 0), 0.0);
                })
                .filter(x -> x._1._3() != 0);
        //checkValues.foreach(x -> System.out.println("x:" + x));
        dailyvalue.foreach(x -> System.out.println("daily:" + x));
        dailyTmpvalue.foreach(x -> System.out.println("tmpdaily:" + x));



        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageThroughDays=
                checkValues
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1/x._2._2));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> standardDeviation =
                checkValues
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, x._1._3())))
                        .join(averageThroughDays)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), new Tuple2<>(Math.pow(x._2._1._1 - x._2._2, 2),  1.0)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), Math.sqrt(x._2._1/(x._2._2-1.0))));

        averageThroughDays.saveAsTextFile(OUTPUT_DIRECTORY + "/query2mean");
        standardDeviation.saveAsTextFile(OUTPUT_DIRECTORY + "/query2standardDeviation");

        TimeClass.getInstance().stop();
    }

    private static void query3(){

        TimeClass.getInstance().start();
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
                Double> plugsStarterValue = dataFiltered
                    .mapToPair(StructureHouses::calculateDay)
                    .reduceByKey((x, y) -> {
                        if(x._1() < y._1())
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
                Double> plugsStarterTmpValue = dataFiltered
                .mapToPair(StructureHouses::calculateDay)
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
                        .mapToPair(StructureHouses::calculateDay)
                        .reduceByKey((x, y) -> {
                            if(x._1() > y._1()) {
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
                        >,
                Double> plugsFinalTmpValues =
                dataFiltered
                        .mapToPair(StructureHouses::calculateDay)
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
                        .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1 - x._2._2));

        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >, Double> dailyTmpvalue =
                plugsFinalTmpValues.join(plugsStarterTmpValue)
                        .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1 - x._2._2));

        JavaPairRDD<Tuple5
                <
                        Integer/*house_id*/,
                        Integer/*plug_id*/,
                        Integer/*timezone*/,
                        Integer/*day*/,
                        Integer/*daytype*/
                        >, Double> finalvalue = dailyTmpvalue.join(dailyvalue)
                .mapToPair(x -> {
                    if (x._2._1.equals( x._2._2))
                        return new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()),x._2._1);
                    else
                        return new Tuple2<>(new Tuple5<>(0, 0, 0, 0, 0), 0.0);
                })
                .filter(x -> x._1._3() != 0);



        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageTopTime =
                finalvalue
                        .filter(x -> x._1._5() == 1)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1()/x._2._2()));

        JavaPairRDD<Tuple2<Integer, Integer>, Double> averageDownTime =
                finalvalue
                        .filter(x -> x._1._5() == 0)
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                        .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), x._2._1/x._2._2));
        averageDownTime.foreach(x -> {
            if (x._2 == 0.0)    //controlling if there is a house that doesn't spent energy in low zone
                System.out.println("x:" + x);
        });

        JavaPairRDD<Tuple2<Integer, Integer>, Double> sorting =
                averageTopTime
                        .join(averageDownTime)
                        .mapToPair(x -> new Tuple2<>(x._2._1 - x._2._2, x._1)) //value first
                        .sortByKey()
                        .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._2._1, x._2._2), x._1));

        sorting.saveAsTextFile(OUTPUT_DIRECTORY + "/query3");

        TimeClass.getInstance().stop();



    }

    /**
     *
     * @param x
     * @return
     */

    private static Tuple2<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Double, Integer>> calculateDay(SorterClass x) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(x.getTimestamp() * 1000);
        int day_of_week = c.get(Calendar.DAY_OF_WEEK);  //taking day of week from timestamp value
        if (x.getTimezone() == 0 ||         //low zone: 00:00 -> 5:59 & 18:00 -> 23:59 & saturday & sunday
                x.getTimezone() == 3 ||
                day_of_week == 1 ||
                day_of_week == 7)
            return new Tuple2<>(new Tuple5<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay(), 0), new Tuple2<>(x.getValue(), x.getTimestamp()));
        else                                //high zone: 6:00 -> 11:59 & 12:00 -> 17:59 & not saturday or sunday
            return new Tuple2<>(new Tuple5<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay(), 1), new Tuple2<>(x.getValue(), x.getTimestamp()));
    }
}
