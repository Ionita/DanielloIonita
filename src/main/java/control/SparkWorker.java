package control;

import entities.SorterClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkWorker {

    private static SparkWorker instance = new SparkWorker();
    private JavaSparkContext sc;

    private SparkWorker(){}

    public static SparkWorker getInstance() {
        return instance;
    }

    /**
     * Function that initializes the spark context
     * @param appname
     * @param mastername
     */
    public void initSparkContext(String appname, String mastername){
        SparkConf conf = new SparkConf().setMaster(mastername).setAppName(appname);
        sc = new JavaSparkContext(conf);
    }

    /**
     * Function that parses tuples and stores them in SorterClass Objects.
     * @param filepath
     * @return
     */
    public JavaRDD<SorterClass> parseFile(String filepath){

        return sc.textFile(filepath).map(
                (Function<String, SorterClass>) line -> {
                    String[] fields = line.split(",");
                    if (fields.length == 7) {
                        SorterClass sd;
                        try {
                            sd = new SorterClass(Integer.parseUnsignedInt(fields[0]),
                                    Integer.parseUnsignedInt(fields[1]),
                                    Double.valueOf(fields[2]),
                                    Integer.valueOf(fields[3]),
                                    Integer.valueOf(fields[4]),
                                    Integer.valueOf(fields[5]),
                                    Integer.valueOf(fields[6]));
                        } catch(Exception e){
                            e.printStackTrace();
                            sd = new SorterClass( 0, 0,0.0,0, 0, 0, 0);
                        }
                        return sd;
                    }
                    else return new SorterClass( 0,0,0.0,0, 0, 0, 0);
                });
    }

    /**
     * Function that closes spark connection
     */
    public void closeConnection(){
        sc.close();
    }

}