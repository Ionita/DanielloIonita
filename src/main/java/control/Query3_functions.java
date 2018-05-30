package control;

import entities.SorterClass;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import runner.StructureHouses;
import scala.Tuple2;
import scala.Tuple5;

import java.util.Calendar;

public class Query3_functions {

    private static Query3_functions instance = new Query3_functions();

    private Query3_functions(){}

    public static Query3_functions getInstance(){
        return instance;
    }


    public JavaPairRDD<Tuple2<Integer, Integer>, Double> q3_sortData(
            JavaPairRDD<Tuple2<Integer, Integer>, Double>  averageLow,
            JavaPairRDD<Tuple2<Integer, Integer>, Double>  averageHigh)
    {
        return averageHigh
                .join(averageLow)
                .mapToPair(x -> new Tuple2<>(x._2._1 - x._2._2, x._1)) //value first
                .sortByKey()
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._2._1, x._2._2), x._1));
    }

    public JavaPairRDD<Tuple5<
            Integer/*house_id*/,
            Integer/*plug_id*/,
            Integer/*timezone*/,
            Integer/*day*/,
            Integer/*daytype*/
            >,
            Double> q3_getPlugStarterValue(JavaRDD<SorterClass> data)
    {
        return data
                .mapToPair(Query3_functions::q3_calculateDay)
                .reduceByKey((x, y) -> {
                    if(x._2() < y._2())
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1()));
    }

    public JavaPairRDD<Tuple5
            <
                    Integer/*house_id*/,
                    Integer/*plug_id*/,
                    Integer/*timezone*/,
                    Integer/*day*/,
                    Integer/*daytype*/
                    >,
            Double> q3_getPlugFinalValue(JavaRDD<SorterClass> data)
    {
        return data
                .mapToPair(Query3_functions::q3_calculateDay)
                .reduceByKey((x, y) -> {
                    if(x._2() > y._2())
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1()));
    }

    public JavaPairRDD<Tuple5
            <
                    Integer/*house_id*/,
                    Integer/*plug_id*/,
                    Integer/*timezone*/,
                    Integer/*day*/,
                    Integer/*daytype*/
                    >, Double> q3_getDailyValue(
            JavaPairRDD<Tuple5
                    <
                            Integer/*house_id*/,
                            Integer/*plug_id*/,
                            Integer/*timezone*/,
                            Integer/*day*/,
                            Integer/*daytype*/
                            >, Double> startervalue,
            JavaPairRDD<Tuple5
                    <
                            Integer/*house_id*/,
                            Integer/*plug_id*/,
                            Integer/*timezone*/,
                            Integer/*day*/,
                            Integer/*daytype*/
                            >, Double> finalvalue)
    {
        return finalvalue.join(startervalue)
                .mapToPair(x -> new Tuple2<>(new Tuple5<>(x._1._1(), x._1._2(), x._1._3(), x._1._4(), x._1._5()), x._2._1 - x._2._2));
    }

    public JavaPairRDD<Tuple2<Integer, Integer>, Double> q3_getAverageForTimeFrame(
            int timeframe,
            JavaPairRDD<Tuple5
                    <
                            Integer/*house_id*/,
                            Integer/*plug_id*/,
                            Integer/*timezone*/,
                            Integer/*day*/,
                            Integer/*daytype*/
                            >, Double> data)
    {
        return data
                .filter(x -> x._1._5() == timeframe)
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1()/x._2._2()));
    }



    private static Tuple2<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Double, Integer>> q3_calculateDay(SorterClass x) {
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
