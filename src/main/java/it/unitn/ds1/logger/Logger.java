package it.unitn.ds1.logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {

    private static Logger instance = null;
    private static String logFilePath = null;

    private BufferedWriter fOut;

    private Logger(String filePath){
        try {
            fOut = new BufferedWriter(new FileWriter(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Logger getInstance(String filePath){
        if (instance == null)
            instance = new Logger(filePath);

        return instance;
    }

    public void log(String coordinatorID, String text){
        try {
            fOut.write(coordinatorID + " --- " + text);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void close(){
        if (fOut != null) {
            try {
                fOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
