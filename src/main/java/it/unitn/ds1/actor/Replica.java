package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.actor.message.*;
import it.unitn.ds1.logger.Logger;

import java.util.*;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 3000; //3 seconds
    private List<ActorRef> group; // the list of peers (the multicast group)
    private ActorRef coordinator;
    private List<ReplicaMessage> buffer; //list of message to ack
    private List<ReplicaMessage> history;
    private int currentEpoch;
    private int currentSeqNumber;
    private Map<Integer, Integer> ack; //COORDINATOR: keep count of every ack before WRITEOK <seq num : ack count >
    private final int id;         // ID of the current actor

    /* -- Actor constructor --------------------------------------------------- */
    //TODO: put boolean "isCoordinator"
    public Replica(int id) {
        this.currentEpoch = 0;
        this.currentSeqNumber = 0;
        this.id = id;
        this.coordinator = null;
        this.buffer = new ArrayList<ReplicaMessage>();
        this.history = new ArrayList<ReplicaMessage>();
        this.ack = new HashMap<Integer, Integer>();
    }

    static public Props props(int id) {
        return Props.create(Replica.class, () -> new Replica(id));
    }

    /* -- Actor behaviour ----------------------------------------------------- */

    // here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientUpdateRequestMsg.class,    this::onClientUpdateRequestMsg)
                .match(ReplicaMessage.class,            this::onReplicaMsg)
                .match(AckMessage.class,                this::onAckMsg)
                .match(WriteOKMessage.class,            this::onWriteOKMsg)
                .match(StartMessage.class,              this::onStartMessage)
                .match(ReadRequestMessage.class,        this::onReadRequest)
                .build();
    }

    /**
     * Invoked when we receive the initial message that contains the clients and replicas groups
     * @param message
     * */
    public void onStartMessage(StartMessage message){
        this.group = message.getReplicaList();

        //The initial coordinator is the first replica of the list
        this.coordinator = this.group.get(0);
    }

    /**
     * Create new ReplicaMessage, put into buffer, send to coordinator,
     * start timer if expire COORDINATOR ELECTION
     * @param msg
     */
    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        currentSeqNumber++;
        ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value, id);
        coordinator.tell(update, getSelf());
        buffer.add(update);
        //TODO: start timer
    }

    /**
     * if I'm coordinator, then broadcast UPDATE to others and send ACK to Replica issuing update,
     * else if I'm a simple Replica register new message in buffer, ACK coordinator
     * @param msg
     */
    public void onReplicaMsg(ReplicaMessage msg) {
        if(coordinator.equals(getSelf())){
            broadCastUpdateRequest(msg);
        }
        else {
            buffer.add(msg);
            if(currentSeqNumber < msg.sequenceNumber) {
                currentSeqNumber = msg.sequenceNumber;
            }
            AckMessage ackMessage = new AckMessage(currentEpoch, currentSeqNumber);
            coordinator.tell(ackMessage, getSelf());
        }
    }

    /**
     * Stop the timeout corresponding to epoch e and sequence number i, then put message <e,i> from buffer to history,
     * if I'm coordinator update the ack counter and check QUORUM if counter is greater or equal to N/2 + 1 then send WRITEOK
     * @param msg
     */
    public void onAckMsg(AckMessage msg) {
        //TODO: stop timeout
        if(coordinator.equals(getSelf())){
            int ackNumber = incrementAck(msg.sequenceNumber);
            if(ackNumber > group.size()/2) {
                WriteOKMessage ok = new WriteOKMessage(currentEpoch, msg.sequenceNumber);
                broadcastMsg(ok, Arrays.asList(getSelf()));
            }
        }
        else {
            //update buffer in case of wrong sequence number/epoch
            ReplicaMessage bufferedMsg = removeMessageFromBuffer(msg.oldSequenceNumber);
            ReplicaMessage updatedBufferMsg = new ReplicaMessage(msg.epoch, msg.sequenceNumber, bufferedMsg.value, bufferedMsg.senderId);
            buffer.add(updatedBufferMsg);
        }
    }

    /**
     * Check on buffer corresponding seq. num. then put into history
     * @param msg
     */
    public void onWriteOKMsg(WriteOKMessage msg) {
        ReplicaMessage bufferedMsg = removeMessageFromBuffer(msg.sequenceNumber);
        history.add(bufferedMsg);
    }

    /**
     * If client request a read then answer with last message in history
     * @param msg
     */
    public void onReadRequest(ReadRequestMessage msg) {
        ReadResponseMessage msgToSend;

        if(history.size() > 0){
            ReplicaMessage currentMsg = this.history.get(this.history.size() - 1);
            msgToSend = new ReadResponseMessage(currentMsg.value);
        }
        else {
            msgToSend = new ReadResponseMessage(-1);
        }
        this.sender().tell(msgToSend, getSelf());

    }


    /* -- Actor utils ----------------------------------------------------- */

    private void logHistory() {
        Logger log = Logger.getInstance();
        for(ReplicaMessage msg : history){
            log.log(msg.toString());
        }
    }

    private ReplicaMessage removeMessageFromBuffer(int sequenceNumber){
        int bufferMsgIndex = -1;
        for(int i=0; i<buffer.size(); i++) {
            ReplicaMessage tmp = buffer.get(i);
            if (tmp.sequenceNumber == sequenceNumber) {
                bufferMsgIndex = i;
            }
        }
        if(bufferMsgIndex == -1){
            return null;
        }
        else{
            return buffer.remove(bufferMsgIndex);
        }
    }

    public void broadCastUpdateRequest(ReplicaMessage msg) {
        currentSeqNumber++;
        AckMessage ackMessage = new AckMessage(currentEpoch, currentSeqNumber, msg.sequenceNumber);
        ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value, msg.senderId);
        broadcastMsg(update, Arrays.asList(getSelf(), group.get(msg.senderId)));
        ack.put(currentSeqNumber, 1);
        group.get(msg.senderId).tell(ackMessage, getSelf());
    }

    public int incrementAck(int sequenceNumber) {
        int ackNumber = ack.get(sequenceNumber);
        ackNumber++;
        ack.put(sequenceNumber, ackNumber);
        return ackNumber;
    }

    public <T> void broadcastMsg(T msg, List<ActorRef> replicasToExclude) {
        for (ActorRef replica : group) {
            if (!replicasToExclude.contains(replica)) {
                replica.tell(msg, getSelf());
            }
        }
    }

}

