package control;

import java.io.*;
import java.util.Date;

public class TimeClass_test {

    private static TimeClass_test instance = new TimeClass_test();
    long timeStart;

    private TimeClass_test(){}

    public static TimeClass_test getInstance() {
        return instance;
    }

    public void start(){
        timeStart = System.currentTimeMillis();
    }

    public void stop(String filename){
        long time = System.currentTimeMillis() - timeStart;
        System.out.println("\n\n\n extimated time spent: " + String.valueOf(time));
        FileWriter pw = null;
        try {
            pw = new FileWriter(new File(filename), true);
            String sb = String.valueOf(time) +
                    '\n';

            pw.write(sb);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
