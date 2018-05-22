package control;

import entities.SorterClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class SparkWorker {

    private static SparkWorker instance = new SparkWorker();
    private JavaSparkContext sc;

    private SparkWorker(){}

    public static SparkWorker getInstance() {
        return instance;
    }

    /**
     * funzione che configura l'ambiente spark. Scrivere "local[*]" come master dovrebbe ottimizzare
     * il numero di partizioni MA CONTROLLA perchè non ho capito bene
     * @param appname
     * @param mastername
     */
    public void initSparkContext(String appname, String mastername){
        SparkConf conf = new SparkConf().setMaster(mastername).setAppName(appname);
        sc = new JavaSparkContext(conf);
    }

    /**
     * funzione che effettua il parsing del file in un oggetto di tipo SorterClass (presente dentro main/java/entities).
     * Il controllo delle tuple errate è fatto male e andrà sicuramente usata un'altra funzione ad hoc
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
                            TimeUnit.SECONDS.sleep(3);
                            sd = new SorterClass( 0, 0,0.0,0, 0, 0, 0);
                        }
                        return sd;
                    }
                    else return new SorterClass( 0,0,0.0,0, 0, 0, 0);
                });
    }

    /**
     * funzione che chiude la connessione spark
     */
    public void closeConnection(){
        sc.close();
    }

}