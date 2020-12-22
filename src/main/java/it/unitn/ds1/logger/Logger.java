package it.unitn.ds1.logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Logger {

    private static Logger instance = null;

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

    public static Logger getInstance(){
        if (instance == null)
            throw new NullPointerException();

        return instance;
    }

    public void log(String coordinatorID, String text){
        if (fOut == null)
            throw new NullPointerException();

        try {
            fOut.write(coordinatorID + " --- " + text + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        if (fOut != null) {
            try {
                fOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}