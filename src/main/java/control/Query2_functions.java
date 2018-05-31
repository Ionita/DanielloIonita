package control;

import entities.SorterClass;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

public class Query2_functions {

    private static Query2_functions instance = new Query2_functions();

    private Query2_functions(){}

    public static Query2_functions getInstance(){
        return instance;
    }

    /**
     * Function that gets in input a JavaRDD of objects "SorterClass" and compute the value related to the
     * smaller timestamp (i.e. the starting value for each plug, house, timezone and day)
     * @param data
     * @return
     */
    public JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> q2_getPlugsMinTimestampValue(JavaRDD<SorterClass> data){
        return data
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2 < y._2)     //timestamp order control
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);
    }

    /**
     * Function that gets in input a JavaRDD of objects "SorterClass" and compute the value related to the
     * greatest timestamp (i.e. the final value for each plug, house, timezone and day)
     * @param data
     * @return
     */
    public JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> q2_getPlugsMaxTimestampValue(JavaRDD<SorterClass> data){
        return data
                .mapToPair(x -> new Tuple2<>(new Tuple4<>(x.getHouseid(), x.getPlugid(), x.getTimezone(), x.getDay()), new Tuple2<>(x.getValue(), x.getTimestamp())))
                .reduceByKey((x, y) -> {
                    if(x._2 > y._2)     //timestamp order control
                        return x;
                    else
                        return y;
                })
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._3(), x._1._4()), x._2._1()))
                .reduceByKey((x , y) -> x+y);
    }

    /**
     * Function that returns the difference between the final and the starter values for each house, plug,
     * timezone and day. This value represents the total power consumption during the related time slot
     * @param startervalue
     * @param finalvalue
     * @return
     */
    public JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> q2_getDailyValue(
            JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> startervalue,
            JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> finalvalue)
    {
        return finalvalue.join(startervalue)
                .mapToPair(x -> new Tuple2<>(new Tuple3<>(x._1._1(), x._1._2(), x._1._3()), x._2._1 - x._2._2));
    }

    /**
     * Function that computes the the total power consumption average through days for each house, and timezone
     * @param value
     * @return
     */
    public JavaPairRDD<Tuple2<Integer, Integer>, Double> q2_computeAverage(JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> value){
        return value
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), x._2._1/x._2._2));
    }

    /**
     * Function that computes the total power consumption standard deviation through days for each house and timezone
     * @param value
     * @param average
     * @return
     */
    public JavaPairRDD<Tuple2<Integer, Integer>, Double> q2_computeStandardDeviation(
            JavaPairRDD<Tuple3<Integer, Integer, Integer>, Double> value,
            JavaPairRDD<Tuple2<Integer, Integer>, Double> average)
    {
        return value
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1(), x._1._2()), new Tuple2<>(x._2, x._1._3())))
                .join(average)
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), new Tuple2<>(Math.pow(x._2._1._1 - x._2._2, 2),  1.0)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1._1, x._1._2), Math.sqrt(x._2._1/(x._2._2-1.0))));
    }

}
