package control;

import main.java.entities.SorterClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.util.List;

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
                            sd = new SorterClass(Integer.valueOf(fields[0]),
                                    Integer.valueOf(fields[1]),
                                    Double.valueOf(fields[2]),
                                    Integer.valueOf(fields[3]),
                                    Integer.valueOf(fields[4]),
                                    Integer.valueOf(fields[5]),
                                    Integer.valueOf(fields[6]));
                        } catch(Exception e){
                            sd = new SorterClass(0,0,0.0,0, 0, 0, 0);
                        }
                        return sd;
                    }
                    else return new SorterClass(0,0,0.0,0, 0, 0, 0);
                });
    }

    /**
     * funzione che semplicemente mappa la classe SorterClass in una coppia chiave valore del tipo
     * valore, id della casa
     * @param data
     * @return
     */
    public JavaPairRDD<Double, Integer> mapKeyValue(JavaRDD<SorterClass> data){
        return data.mapToPair((x) -> new Tuple2<>(x.getValue(),x.getHouseid()));
    }

    /**
     * funzione che ordina in modo decrescente un oggetto JavaRDD SorterClass in base all'attributo valore.
     * Probabilmente è più utile su un JavaPairRDD ma intanto questo è un test per la funzionalità di sorting
     * che si trova già dentro Spark
     * @param data
     * @return
     */
    public JavaRDD<SorterClass> sortSorterClass(JavaRDD<SorterClass> data){
        return data.sortBy((Function<SorterClass, Double>) SorterClass::getValue, false, 1 );
    }

    /**
     * funzione che salva una tupla spark Double Integer e la salva in una directory creata ad hoc
     * (LA DISTRUGGE SE GIÀ ESISTE QUINDI STARE ATTENTI). Il formato di salvataggio è in pratica quello di hadoop
     * @param data
     * @param filepath
     */
    public void saveToFile(JavaPairRDD<Double, Integer> data, String filepath){
        File directory = new File(filepath);
        if(directory.exists()){
            String[] entries = directory.list();
            assert entries != null;
            for(String s: entries){
                File currentFile = new File(directory.getPath(),s);
                currentFile.delete();
            }
            directory.delete();
        }
        data.saveAsTextFile(filepath);
    }

    /**
     * funzione che chiude la connessione spark
     */
    public void closeConnection(){
        sc.close();
    }

    /**
     * funzione che ritorna una stupida media tra tutti i valori. è solo un esempio di utilizzo del metodo mean
     * messo a disposizione da spark. Per una media più accurata vedere getAverageByKey
     * @param data
     * @return
     */
    @Deprecated
    public double returnMean(JavaRDD<SorterClass> data){
        return data.mapToDouble(SorterClass::getValue).mean();
    }

    /**
     * funzione di esempio per la classe filter. Ritorna tutti i valori con value maggiore di un
     * determinato valore
     * @param data
     * @param value
     * @return
     */
    public JavaRDD<SorterClass> getValuesOver(JavaRDD<SorterClass> data , double value){
        return data.filter(x -> x.getValue() >= value);
    }

    /**
     * funzione che ritorna solo le tuple caratterizzate da valore istantaneo
     * @param data
     * @return
     */
    public JavaRDD<SorterClass> getInstaTuple(JavaRDD<SorterClass> data) {
        return data.filter(x-> x.isProperty() == 1);
    }

    /**
     * funzione che ritorna solo le tuple caratterizzate da valore istantaneo > 350
     * @param data
     * @return
     */
    public JavaRDD<SorterClass> getQuery1Tuple(JavaRDD<SorterClass> data, double value) {
        return data.filter(x-> x.isProperty() == 1 && x.getValue() >= value);
    }

    /**
     * funzione che ritorna solo le tuple caratterizzate da valore totale
     * @param data
     * @return
     */
    public JavaRDD<SorterClass> getTotalTuple(JavaRDD<SorterClass> data) {
        return data.filter(x-> x.isProperty() == 0);
    }

    /**
     * funzione che effettua lo swap e prende solo le chiavi uniche (i doppioni vengono scartati stupidamente)
     * @param data
     * @return
     */
    public JavaPairRDD<Integer, Double> uniqueKey(JavaPairRDD<Double, Integer> data){
        JavaPairRDD<Integer, Double> swappedKeyValue = data.mapToPair(Tuple2::swap);
        return swappedKeyValue.reduceByKey((x, y) -> x);
    }

    /**
     * funzione che effettua lo swapping e calcola la media dei valori per chiave
     * @param data
     * @return
     */
    public JavaPairRDD<Integer, Double> getAverageByKey(JavaPairRDD<Double, Integer> data){
        JavaPairRDD<Integer, Double> swappedKeyValue = data.mapToPair(Tuple2::swap);
        JavaPairRDD<Integer, Tuple2<Double, Integer>> valueCount = swappedKeyValue.mapValues(value -> new Tuple2<>(value, 1));
        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
        return reducedCount.mapToPair(getAverageByKey);
    }

    private static PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>,Integer,Double> getAverageByKey = (tuple) -> {
        Tuple2<Double, Integer> val = tuple._2;
        double total = val._1;
        int count = val._2;
        return new Tuple2<>(tuple._1, total / count);
    };
}