package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds1.logger.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("ds1project");

        //Start the logger
        Logger.getInstance("log.txt");

        // Create the coordinator
        /*ActorRef coordinator = system.actorOf(Coordinator.props(), "coordinator");

        // Create participants
        List<ActorRef> group = new ArrayList<>();
        for (int i=0; i<N_PARTICIPANTS; i++) {
            group.add(system.actorOf(Participant.props(i), "participant" + i));
        }

        // Send start messages to the participants to inform them of the group
        StartMessage start = new StartMessage(group);
        for (ActorRef peer: group) {
            peer.tell(start, null);
        }

        // Send the start messages to the coordinator
        coordinator.tell(start, null);*/

        try {
            System.out.println("--- Press any key to terminate ---");
            System.in.read();
        }
        catch (IOException ignored) {}

        //Terminate the system and close the log file
        Logger.getInstance().close();
        system.terminate();
    }

}
