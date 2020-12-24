package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds1.logger.Logger;
import it.unitn.ds1.message.AckMessage;
import it.unitn.ds1.message.ReplicaMessage;
import it.unitn.ds1.message.ClientUpdateRequestMsg;
import it.unitn.ds1.message.WriteOKMessage;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 3000; //3 seconds
    private ArrayList<ActorRef> group; // the list of peers (the multicast group)
    private ActorRef coordinator;
    private ArrayList<ReplicaMessage> buffer; //list of message to ack
    private ArrayList<ReplicaMessage> history;
    private int currentEpoch;
    private int currentSeqNumber;
    private Map<Integer, Integer> ack; //COORDINATOR: keep count of every ack before WRITEOK <seq num : ack count >
    private final int id;         // ID of the current actor

    /* -- Actor constructor --------------------------------------------------- */

    public Replica(int id) {
        this.currentEpoch = 0;
        this.currentSeqNumber = 0;
        this.id = id;
        this.coordinator = null;
        this.buffer = new ArrayList<ReplicaMessage>();
        this.history = new ArrayList<ReplicaMessage>();
        this.ack = new HashMap<Integer, Integer>();
    }

    static public Props props(int id, String topic) {
        return Props.create(Replica.class, () -> new Replica(id));
    }

    /* -- Actor behaviour ----------------------------------------------------- */

    // here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClientUpdateRequestMsg.class, this::onClientUpdateRequestMsg)
                .match(ReplicaMessage.class, this::onReplicaMsg)
                .match(AckMessage.class, this::onAckMsg)
                .match(WriteOKMessage.class, this::onWriteOKMsg)
                .build();
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
            currentSeqNumber++;
            AckMessage ackMessage = new AckMessage(currentEpoch, currentSeqNumber, msg.sequenceNumber);
            ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value, msg.senderId);
            for(ActorRef replica: group){
                if(replica.equals(getSelf()) && !replica.equals(group.get(msg.senderId))) {
                    replica.tell(update, getSelf());
                }
            }
            group.get(msg.senderId).tell(ackMessage, getSelf());
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
            int ackNumber = ack.get(msg.sequenceNumber);
            ackNumber++;
            ack.put(msg.sequenceNumber, ackNumber);
            if(ackNumber > group.size()/2) {
                WriteOKMessage ok = new WriteOKMessage(currentEpoch, msg.sequenceNumber);
                for(ActorRef peer : group) {
                    if(!peer.equals(getSelf())){
                        peer.tell(ok, getSelf());
                    }
                }
            }
        }
        else {
            int bufferMsgIndex = -1;
            for(int i=0; i<buffer.size(); i++) {
                ReplicaMessage tmp = buffer.get(i);
                if (tmp.sequenceNumber == msg.oldSequenceNumber) {
                    bufferMsgIndex = i;
                }
            }
            //update buffer in case of wrong sequence number/epoch
            ReplicaMessage bufferedMsg = buffer.remove(bufferMsgIndex);
            ReplicaMessage updatedBufferMsg = new ReplicaMessage(msg.epoch, msg.sequenceNumber, bufferedMsg.value, bufferedMsg.senderId);
            buffer.add(updatedBufferMsg);
        }
    }

    /**
     * Check on buffer corresponding seq. num. then put into history
     * @param msg
     */
    public void onWriteOKMsg(WriteOKMessage msg) {
        int bufferMsgIndex = -1;
        for(int i=0; i<buffer.size(); i++) {
            ReplicaMessage tmp = buffer.get(i);
            if (tmp.sequenceNumber == msg.sequenceNumber) {
                bufferMsgIndex = i;
            }
        }
        ReplicaMessage bufferedMsg = buffer.remove(bufferMsgIndex);
        history.add(bufferedMsg);
    }


    /* -- Actor utils ----------------------------------------------------- */

    private void logHistory() {
        Logger log = Logger.getInstance();
        for(ReplicaMessage msg : history){
            log.log(msg.toString());
        }
    }

}

