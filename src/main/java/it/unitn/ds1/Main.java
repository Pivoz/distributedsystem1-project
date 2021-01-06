package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.actor.Client;
import it.unitn.ds1.actor.Replica;
import it.unitn.ds1.actor.message.StartMessage;
import it.unitn.ds1.logger.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static final int N_CLIENTS = 10;
    public static final int N_REPLICAS = 10;

    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("ds1project");

        //Start the logger
        Logger.getInstance("log.txt");

        //Create the replicas
        List<ActorRef> replicaList = new ArrayList<>();
        for (int i=0; i<N_REPLICAS; i++)
            replicaList.add(system.actorOf(Replica.props(i), "replica" + i));

        //Create the clients
        List<ActorRef> clientList = new ArrayList<>();
        for (int i=0; i<N_CLIENTS; i++)
            clientList.add(system.actorOf(Client.props(i), "client" + i));

        //Send the StartMessage to all the nodes
        StartMessage startMessage = new StartMessage(clientList, replicaList);
        for (ActorRef actor : replicaList)
            actor.tell(startMessage, null);
        for (ActorRef actor : clientList)
            actor.tell(startMessage, null);

        try {
            System.out.println("--- Press any key to terminate ---");
            System.in.read();
        }
        catch (IOException ignored) {}

        //Terminate the system and close the log file
        system.terminate();
        Logger.getInstance().close();
    }

}
