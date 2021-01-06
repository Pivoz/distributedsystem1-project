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
    private final int MAX_READ_ATTEMPTS = 2;

    private List<ActorRef> replicaList;
    private Cancellable timeout = null;
    private byte [] readAttemptsPerReplica;

    private Client(int clientID){
        super();
        this.id = clientID;
        this.readAttemptsPerReplica = new byte[Main.N_REPLICAS];

        for (int i=0; i<readAttemptsPerReplica.length; i++)
            readAttemptsPerReplica[i] = 0;
    }

    public static Props props(int clientID){
        return Props.create(Client.class, () -> new Client(clientID));
    }

    /**
     * Invoked when we receive the initial message that contains the clients and replicas groups.
     * Init the List of ActorRef of this Replica and start a periodic timeout to send to itself a SendClientMessage
     * @param message : StartMessage from main
     * */
    private void onStartMessage(StartMessage message){
        this.replicaList = new ArrayList<>(message.getReplicaList());

        //Start the message scheduling
        getContext().system().scheduler().scheduleWithFixedDelay(
            Duration.create(id, TimeUnit.SECONDS),                        // when to start generating messages
            Duration.create(MESSAGE_INTERVAL_SECONDS, TimeUnit.SECONDS),  // how frequently generate them
            getSelf(),                                                    // destination actor reference
            new SendClientMessage(),                                      // the message to send
            getContext().system().dispatcher(),                           // system dispatcher
            getSelf()                                                     // source of the message (myself)
        );
    }

    /**
    * Invoked when the client receives from itself a SendClientMessage to send a new request to one replica (read or update).
     * @param message : the SendClientMessage sent by the recurrent timeout set in onStartMessage() method.
    * */
    private void onSendClientMessage(SendClientMessage message) {

        if (replicaList.isEmpty()){
            System.err.println("[" + getSelf().path().name() + "] no replicas available");
            return;
        }

        //Pr(read)=0.9, Pr(update)=0.1
        int dice = (int) (Math.random() * 10);
        int replicaPosition;
        do {
            replicaPosition = (int) (Math.random() * replicaList.size());
        } while (replicaList.get(replicaPosition) == null);

        if (dice < 9) {
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
    * Invoked when arrives a read response from a replica. It contains the read value.
     * @param message : the message sent by the replica to which the client had sent the ReadRequestMessage
    * */
    private void onReadResponseMessage(ReadResponseMessage message){
        System.out.println("[" + getSelf().path().name() + "] read value " + message.getValue());

        //Cancel the pending timeout
        if (timeout != null){
            timeout.cancel();
            timeout = null;
        }

        //Reset it's read attempts
        readAttemptsPerReplica[getIdFromString(getSender().path().name())] = 0;
    }

    /**
     * Invoked when the read timeout expires due to the fact the replica does not answer in time.
     * @param message : the ClientReadTimeout message sent by the timeout set before sending the ReadRequestMessage to the replica
     * */
    private void onClientReadTimeoutMessage(ClientReadTimeout message){
        readAttemptsPerReplica[message.getReplicaPosition()]++;
        if (readAttemptsPerReplica[message.getReplicaPosition()] == MAX_READ_ATTEMPTS) {
            System.err.println("[" + getSelf().path().name() + "] Replica " + message.getReplicaPosition() + " removed from replicas list after " + MAX_READ_ATTEMPTS + " failed attempts");
            replicaList.set(message.getReplicaPosition(), null);
        }

        timeout = null;
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

    /**
     * This method extrapolate the final integer characters of the input string and returns them as a byte value
     * @param text : the input text (e.g., "replica12")
     * @return byte value (e.g., 12)
     * */
    private static byte getIdFromString(String text){
        for (int i=0; i<text.length(); i++)
            if (text.charAt(i) >= '0' && text.charAt(i) <= '9')
                return Byte.parseByte(text.substring(i));

        return -1;
    }
}
