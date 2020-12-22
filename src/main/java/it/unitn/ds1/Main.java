package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        // Create the actor system
        final ActorSystem system = ActorSystem.create("helloakka");

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
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        }
        catch (IOException ignored) {}
        system.terminate();
    }

}
