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
        if (filePath == null && instance == null)
            throw new NullPointerException();

        if (instance == null)
            instance = new Logger(filePath);

        return instance;
    }

    public static Logger getInstance(){
        return getInstance(null);
    }

    public void log(String rawString){
        if (fOut == null)
            throw new NullPointerException();

        try {
            fOut.write(rawString + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void logReplicaUpdate(String replicaID, int epoch, int seqNumber, int value){
        this.log("Replica " + replicaID + " update " + epoch + ":" + seqNumber + " " + value);
    }

    public void logReadRequest(String clientID, String replicaID){
        this.log("Client " + clientID + " read req to " + replicaID);
    }

    public void logReadResult(String clientID, int value){
        this.log("Client " + clientID + " read done " + value);
    }

    public void close(){
        if (fOut != null) {
            try {
                fOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        instance = null;
    }

}