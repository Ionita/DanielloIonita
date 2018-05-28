package control;

public class TimeClass {

    private static TimeClass instance = new TimeClass();
    long timeStart;

    private TimeClass(){}

    public static TimeClass getInstance() {
        return instance;
    }

    public void start(){
        timeStart = System.currentTimeMillis();
    }

    public void stop(){
        long time = System.currentTimeMillis() - timeStart;
        System.out.println("\n\n\ntime spent: \t" + String.valueOf(time));

    }
}
