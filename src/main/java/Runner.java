import control.SparkWorker;
import main.java.control.TimeClass;
import main.java.entities.SorterClass;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


public class Runner {

    public static void main(String[] args) {
        newStartFlow();
    }

    private static void newStartFlow(){
//        simpleSorterByValue();
//        simpleValueAverage();
//        getValuesUnderCondition();
//        getAverageHouses();
//        query1();
        query2();
    }

    /**
     * funzione test utilizzata per effettuare un semplice sorting in base al valore.
     * Ritorna un insieme di coppie id-tupla, valore ordinate su un file creato ad-hoc
     */
    private static void simpleSorterByValue(){
        SparkWorker.getInstance().initSparkContext("simple sorter", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        data = SparkWorker.getInstance().sortSorterClass(data);
        JavaPairRDD<Double, Integer> keyvaluepair = SparkWorker.getInstance().mapKeyValue(data);
        SparkWorker.getInstance().saveToFile(keyvaluepair, "sorting");
        SparkWorker.getInstance().closeConnection();
    }

    /**
     * funzione test che ritorna una media (inutile) tra tutti i valori presenti nel file.
     * Stampa il risultato in formato double in standard output
     */
    private static void simpleValueAverage(){
        SparkWorker.getInstance().initSparkContext("simple average", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        System.out.println(SparkWorker.getInstance().returnMean(data));
        SparkWorker.getInstance().closeConnection();
    }

    /**
     * funzione test che ritorna tutte le tuple con valore maggiore a 350. è perlopiù inutile e ritorna
     * in un file creato appositamente
     */
    private static void getValuesUnderCondition(){
        SparkWorker.getInstance().initSparkContext("values under condition", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        data = SparkWorker.getInstance().getValuesOver(data, 350.0);
        JavaPairRDD<Double, Integer> keyvaluepair = SparkWorker.getInstance().mapKeyValue(data);
        SparkWorker.getInstance().saveToFile(keyvaluepair, "condition");
        SparkWorker.getInstance().closeConnection();
    }

    /**
     * funzione test che calcola la media delle singole abitazioni stampandole in standard output.
     * è molto simile alla query numero due ma non distingue le fasce orarie
     */
    private static void getAverageHouses(){
        SparkWorker.getInstance().initSparkContext("query1", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        data = SparkWorker.getInstance().getTotalTuple(data);
        JavaPairRDD<Double, Integer> keyvalue = SparkWorker.getInstance().mapKeyValue(data);
        JavaPairRDD<Integer, Double> averageItems = SparkWorker.getInstance().getAverageByKey(keyvalue);
        averageItems.foreach(item-> System.out.println("key: " + item._1 + ", average value: " + item._2));
    }

    /**
     * funzione che simula il comportamento da ottenere con la query numero 1. Non è testata, non è ottimizzata
     * e non so se funziona realmente. Soprattutto, non so se ho capito bene la richiesta della query 1.
     * Stampa il risultato in standard output e su un file specificato nella funzione stop()
     */
    private static void query1(){
        TimeClass.getInstance().start();
        SparkWorker.getInstance().initSparkContext("query1", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        //data = SparkWorker.getInstance().getInstaTuple(data);
        //data = SparkWorker.getInstance().getValuesOver(data, 350.0);
        data = SparkWorker.getInstance().getQuery1Tuple(data, 350.0);
        JavaPairRDD<Double, Integer> keyvalue = SparkWorker.getInstance().mapKeyValue(data);
        JavaPairRDD<Integer, Double> swappedkeyvalue = SparkWorker.getInstance().uniqueKey(keyvalue);
        swappedkeyvalue.foreach(item -> System.out.println(item._1));
        SparkWorker.getInstance().closeConnection();
        TimeClass.getInstance().stop("query1results.csv");
    }

    private static void query2(){
        TimeClass.getInstance().start();
        SparkWorker.getInstance().initSparkContext("query1", "local[*]");
        JavaRDD<SorterClass> data = SparkWorker.getInstance().parseFile("textfiles/d14_filtered.csv");
        SparkWorker.getInstance().closeConnection();
        TimeClass.getInstance().stop("query1results.csv");
    }

}
