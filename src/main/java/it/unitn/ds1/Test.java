package it.unitn.ds1;

import java.io.*;

/*
* The read operations must respect sequential consistency
* */
public class Test {

    public static void main(String[] args) throws IOException {

        final String FILEPATH = "log.txt";
        File log = new File(FILEPATH);
        if (!log.exists()){
            System.err.println("ERROR: " + FILEPATH + " does not exists");
            System.exit(1);
        }

        BufferedReader fIn = new BufferedReader(new FileReader(log));
        String line = fIn.readLine();
        while (line != null){
            //TODO: implement check logic

            line = fIn.readLine();
        }

        fIn.close();
        System.exit(0);
    }

}
