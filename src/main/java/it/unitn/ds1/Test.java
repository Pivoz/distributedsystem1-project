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
        int lineNumber = 1;

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
                    else
                        System.out.println("WARN: founded a read value not paired with a read request at line n." + lineNumber);
                }
                else {
                    System.err.println("ERROR: malformed logged string found!\nThe string is: " + line);
                }
            }

            line = fIn.readLine();
            lineNumber++;
        }

        fIn.close();

        for (int i=0; i<replicaList.size(); i++)
            System.out.println("Replica " + i + " READs sequence: " + listToString(replicaList.get(i)));

        //Data elaboration
        for (int i=0; i<replicaList.size() - 1; i++){
            for (int j=i+1; j<replicaList.size(); j++){

                //Find common object between replica i and j, ordered in replica i order
                List<Integer> commonObjects = new ArrayList<>();
                for (Integer value : replicaList.get(i))
                    if (replicaList.get(j).contains(value))
                        commonObjects.add(value);

                if (DEBUG)
                    System.out.println("COMMON (" + i+", "+j+"): " + listToString(commonObjects));

                //Check the order of the common objects
                if (!commonObjects.isEmpty())
                    for (int k=0; k<commonObjects.size(); k++)
                        if (!checkPreviousCommonObject(replicaList.get(j), replicaList.get(j).indexOf(commonObjects.get(k))-1, (k==0) ? null : commonObjects.get(k-1), commonObjects)){
                            System.err.println("SEQUENTIAL CONSISTENCY VIOLATED!\nViolation found between replica n." + i + " and replica n." + j);
                            System.exit(0);
                        }
            }
        }

        System.out.println("\nSequential consistency check passed without errors");
        System.exit(0);
    }

    private static boolean checkPreviousCommonObject(List<Integer> list, int end, Integer valueToCheck, List<Integer> commonObjects){

        for (int i=end; i>=0; i--)
            if (commonObjects.contains(list.get(i))) {
                if (valueToCheck == null || !list.get(i).equals(valueToCheck))
                    return false;
                else
                    return true;
            }
        return true;
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
