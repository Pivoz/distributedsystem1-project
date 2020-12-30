package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.Main;
import it.unitn.ds1.actor.message.*;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private int id;
    private final int MESSAGE_INTERVAL_SECONDS = 2;
    private final int READ_TIMEOUT_SECONDS = Main.N_REPLICAS;

    private List<ActorRef> replicaList;
    private Cancellable timeout = null;

    private Client(int clientID){
        super();
        this.id = clientID;
    }

    public static Props props(int clientID){
        return Props.create(Client.class, () -> new Client(clientID));
    }

    /**
     * Returns the id of the client
     * @return id
     * */
    public int getId(){ return id; }

    /**
     * Invoked at the beginning to initialize the client
     * @param message
     */
    private void onStartMessage(StartMessage message){
        this.replicaList = new ArrayList<>(message.getReplicaList());

        //Start the message scheduling
        Cancellable timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(id, TimeUnit.SECONDS),                   // when to start generating messages
                Duration.create(MESSAGE_INTERVAL_SECONDS, TimeUnit.SECONDS),  // how frequently generate them
                getSelf(),                                                    // destination actor reference
                new SendClientMessage(),                                      // the message to send
                getContext().system().dispatcher(),                           // system dispatcher
                getSelf()                                                     // source of the message (myself)
        );
    }

    /**
    * Invoked when the client have to schedule a new request to one replica
     * @param message
    * */
    private void onSendClientMessage(SendClientMessage message) {

        if (replicaList.isEmpty()){
            System.err.println("[" + getSelf().path().name() + "] no replicas available");
            return;
        }

        //Pr(read)=0.8, Pr(update)=0.2
        int dice = (int) (Math.random() * 10);
        int replicaPosition;
        do {
            replicaPosition = (int) (Math.random() * replicaList.size());
        } while (replicaList.get(replicaPosition) == null);

        if (dice < 8) {
            //If the timeout isn't null there is a pending operation -> skip
            if (timeout != null)
                return;

            replicaList.get(replicaPosition).tell(new ReadRequestMessage(), getSelf());
            System.out.println("[" + getSelf().path().name() + "] raised a read request to " + replicaList.get(replicaPosition).path().name());

            timeout = getContext().system().scheduler().scheduleOnce(
                    Duration.create(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    getSelf(),
                    new ClientReadTimeout(replicaPosition),
                    getContext().system().dispatcher(),
                    getSelf()
            );
        }
        else {
            int newValue = (int) (Math.random() * Integer.MAX_VALUE);
            replicaList.get(replicaPosition).tell(new ClientUpdateRequestMsg(newValue), getSelf());
            System.out.println("[" + getSelf().path().name() + "] raised an update request (newValue = " + newValue + ") to " + replicaList.get(replicaPosition).path().name());
        }
    }

    /**
    * Invoked when arrives a read response from a replica
     * @param message
    * */
    private void onReadResponseMessage(ReadResponseMessage message){
        System.out.println("[" + getSelf().path().name() + "] read value " + message.getValue());

        //Cancel the pending timeout
        if (timeout != null){
            timeout.cancel();
            timeout = null;
        }
    }

    /**
     * Invoked when the read timeout expires
     * @param message
     * */
    private void onClientReadTimeoutMessage(ClientReadTimeout message){
        System.err.println("[" + getSelf().path().name() + "] seems that replica " + message.getReplicaPosition() + " is not working properly. Removed from the replica list");

        timeout = null;
        replicaList.set(message.getReplicaPosition(), null);
    }

    /**
    * Message receiver builder
    * */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SendClientMessage.class,     this::onSendClientMessage)
                .match(StartMessage.class,          this::onStartMessage)
                .match(ReadResponseMessage.class,   this::onReadResponseMessage)
                .match(ClientReadTimeout.class,     this::onClientReadTimeoutMessage)
                .build();
    }
}
