package it.unitn.ds1;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
* The read operations must respect sequential consistency.
* All processes see the same interleaving
* */
public class Test {

    private static final boolean DEBUG = true;

    public static void main(String[] args) throws IOException {
        //TODO: fix this shit
        final String FILEPATH = "log.txt";
        File log = new File(FILEPATH);
        if (!log.exists()){
            System.err.println("ERROR: " + FILEPATH + " does not exists");
            System.exit(1);
        }

        //Data structure initialization
        List<List<Integer>> replicaList = new ArrayList<>();
        for (int i=0; i<Main.N_REPLICAS; i++)
            replicaList.add(new ArrayList<>());

        byte [] clientPendingReads = new byte[Main.N_CLIENTS];
        for (int i=0; i<clientPendingReads.length; i++)
            clientPendingReads[i] = -1;

        //Read the log file line by line
        BufferedReader fIn = new BufferedReader(new FileReader(log));
        String line = fIn.readLine();
        while (line != null){
            if (line.startsWith("Client")){
                String [] splitted = line.split(" ");

                if (splitted.length == 6){
                    //Read request
                    byte clientID = getIdFromString(splitted[1]);
                    byte replicaID = getIdFromString(splitted[5]);

                    clientPendingReads[clientID] = replicaID;
                }
                else if (splitted.length == 5) {
                    //Read value
                    byte clientID = getIdFromString(splitted[1]);
                    int value = Integer.parseInt(splitted[4]);
                    byte replicaID = clientPendingReads[clientID];

                    //If there is a pending read for that client
                    if (replicaID != -1){
                        clientPendingReads[clientID] = -1;

                        //If the read element was not already inserted in the list of that replica
                        if ((replicaList.get(replicaID).size() == 0 || replicaList.get(replicaID).get(replicaList.get(replicaID).size()-1) != value) && value != -1)
                            replicaList.get(replicaID).add(value);
                    }
                }
                else {
                    System.err.println("ERROR: malformed logged string found!\nThe string is: " + line);
                }
            }

            line = fIn.readLine();
        }

        fIn.close();

        for (int i=0; i<replicaList.size(); i++)
            System.out.println("Replica " + i + " output: " + listToString(replicaList.get(i)));

        int largestList = 0;
        for (int i=1; i<replicaList.size(); i++)
            if (replicaList.get(i).size() > replicaList.get(largestList).size())
                largestList = i;

        //Data elaboration
        List<Integer> sequentialList = new ArrayList();
        for (Integer value : replicaList.get(largestList))
            sequentialList.add(value);

        if (DEBUG)
            System.out.println("LIST = " + listToString(sequentialList));

        for (int i=0; i<replicaList.size(); i++){

            if (i == largestList)
                continue;

            List actualListRef = replicaList.get(i);
            int lastIndex = -1;

            for (int j=0; j<actualListRef.size(); j++){
                int index = sequentialList.indexOf(actualListRef.get(j));

                if (index == -1){
                    //There isn't that element in the global list -> I add it
                    if (j==0) {
                        sequentialList.add(0, (Integer) actualListRef.get(j));
                        lastIndex = 0;
                    }
                    else {
                        int lastCommonObjectPosition = sequentialList.indexOf(actualListRef.get(j-1));
                        sequentialList.add(lastCommonObjectPosition+1, (Integer) actualListRef.get(j));
                        lastIndex = lastCommonObjectPosition + 1;
                    }
                }
                else if (index < lastIndex){
                    System.err.println("SEQUENTIAL CONSISTENCY VIOLATED!\nViolation found in replica n." + i + " step n." + j);
                    System.exit(0);
                }
                else
                    lastIndex = index;
            }

            if (DEBUG)
                System.out.println("LIST = " + listToString(sequentialList));
        }

        if (DEBUG)
            System.out.println("LIST = " + listToString(sequentialList));

        System.out.println("Sequential consistency check passed without errors");
        System.exit(0);
    }

    private static byte getIdFromString(String text){
        for (int i=0; i<text.length(); i++)
            if (text.charAt(i) >= '0' && text.charAt(i) <= '9')
                return Byte.parseByte(text.substring(i));

        return -1;
    }

    private static String listToString(List list){
        String ret = "[";
        for (int i=0; i<list.size(); i++){
            if (i != 0)
                ret += ",";
            ret += list.get(i);
        }
        return ret + "]";
    }

}
