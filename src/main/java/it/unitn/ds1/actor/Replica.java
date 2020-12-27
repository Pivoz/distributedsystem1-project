package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.actor.message.*;
import it.unitn.ds1.logger.Logger;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 3000; //3 seconds
    private final int IS_ALIVE_TIMEOUT = 1500;
    private List<ActorRef> group; // the list of peers (the multicast group)
    private ActorRef coordinator;
    private List<Integer> buffer; //list of message to ack
    private List<ReplicaMessage> history;
    private int currentEpoch;
    private int currentSeqNumber;
    private Map<Integer, Integer> ack; //COORDINATOR: keep count of every ack before WRITEOK <seq num : ack count >
    private final int id;         // ID of the current actor
    private boolean isElectionInProgress;
    private Cancellable isAliveTimer;
    private Map<Integer, Cancellable> updateRequestTimers;
    private Cancellable electionTimer;

    /* -- Actor constructor --------------------------------------------------- */
    //TODO: put boolean "isCoordinator"
    public Replica(int id) {
        this.currentEpoch = 0;
        this.currentSeqNumber = 0;
        this.id = id;
        this.coordinator = null;
        this.buffer = new ArrayList<Integer>();
        this.history = new ArrayList<ReplicaMessage>();
        this.ack = new HashMap<Integer, Integer>();
        this.isElectionInProgress = false;
        this.isAliveTimer = null;
        this.updateRequestTimers = new HashMap<Integer, Cancellable>();
        this.electionTimer = null;
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
                .match(ElectionMsg.class,               this::onElectionMsg)
                .match(SyncMsg.class,                   this::onSyncMsg)
                .match(IsAliveMsg.class,                this::onIsAliveMsg)
                .build();
    }

    /**
     * Invoked when we receive the initial message that contains the clients and replicas groups
     * @param message
     * */
    public void onStartMessage(StartMessage message){
        this.group = message.getReplicaList();
        //The initial coordinator is the first replica of the list
        coordinator = this.group.get(0);
        currentEpoch++;

        if(coordinator.equals(getSelf())) {
            isAliveTimer = setTimeout(IS_ALIVE_TIMEOUT, getSelf(), new IsAliveMsg(), true);
        }
        else{
            isAliveTimer = setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), new ElectionMsg(new ArrayList<Integer>(group.size())), false);
        }
    }

    /**
     * Create new ReplicaMessage, put into buffer, send to coordinator,
     * start timer if expire COORDINATOR ELECTION
     * @param msg
     */
    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        ReplicaMessage update = new ReplicaMessage(msg.value);
        coordinator.tell(update, getSelf());
        updateRequestTimers.put(msg.value, setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), new ElectionMsg(new ArrayList<Integer>(group.size())), false));
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
            buffer.add(msg.value);
            if(currentSeqNumber < msg.sequenceNumber) {
                currentSeqNumber = msg.sequenceNumber;
            }
            AckMessage ackMessage = new AckMessage(msg.value, AckType.COORDUPDATEREQUEST);
            coordinator.tell(ackMessage, getSelf());
        }
    }

    /**
     * Stop the timeout corresponding to epoch e and sequence number i, then put message <e,i> from buffer to history,
     * if I'm coordinator update the ack counter and check QUORUM if counter is greater or equal to N/2 + 1 then send WRITEOK
     * @param msg
     */
    public void onAckMsg(AckMessage msg) {
        switch (msg.ackType){
            case UPDATEREQUEST:
                if(this.updateRequestTimers.containsKey(msg.value)) {
                    this.updateRequestTimers.remove(msg.value).cancel();
                }
                break;
            case COORDUPDATEREQUEST:
                int ackNumber = incrementAck(msg.value);
                if(ackNumber == group.size()/2 + 1) {
                    currentSeqNumber++;
                    WriteOKMessage ok = new WriteOKMessage(currentSeqNumber, msg.value);
                    int bufferedMsg = removeMessageFromBuffer(msg.value);
                    ReplicaMessage replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, msg.value);
                    history.add(replicaMsg);
                    broadcastMsg(ok, Arrays.asList(getSelf()));
                    Logger.getInstance().logReplicaUpdate(getSelf().path().name(), currentEpoch, currentSeqNumber, replicaMsg.value);
                }
                break;
            case ELECTION:
                if(electionTimer != null){
                    electionTimer.cancel();
                    electionTimer = null;
                }
                break;
        }
    }

    /**
     * Check on buffer corresponding seq. num. then put into history
     * @param msg
     */
    public void onWriteOKMsg(WriteOKMessage msg) {
        currentSeqNumber = msg.sequenceNumber;
        int bufferedMsg = removeMessageFromBuffer(msg.value);
        ReplicaMessage replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, bufferedMsg);
        history.add(replicaMsg);

        //System.out.println("REPLICA " + id + "- Add to history: " + replicaMsg.value);
        Logger.getInstance().logReplicaUpdate(getSelf().path().name(), currentEpoch, currentSeqNumber, replicaMsg.value);
    }

    /**
     * If client request a read then answer with last message in history
     * @param msg
     */
    public void onReadRequest(ReadRequestMessage msg) {
        ReadResponseMessage msgToSend;
        Logger logger = Logger.getInstance();
        logger.logReadRequest(this.sender().path().name(), getSelf().path().name());

        if(history.size() > 0){
            ReplicaMessage currentMsg = this.history.get(this.history.size() - 1);
            msgToSend = new ReadResponseMessage(currentMsg.value);
        }
        else {
            msgToSend = new ReadResponseMessage(-1);
        }
        this.sender().tell(msgToSend, getSelf());
        logger.logReadResult(this.sender().path().name(), msgToSend.getValue());

    }

    /**
     * If Election in progress check if max seq number is mine, if yes then broadcast SYNC with me as coord, otherwise
     * forward the Election
     * If Election not in progress, set current Replica last seq number and forward
     * Finally ACK
     * @param msg
     */
    private void onElectionMsg(ElectionMsg msg) {
        if(isElectionInProgress){
            Integer maxSeqNumber = Collections.max(msg.lastSequenceNumberPerActor);
            Integer maxIndex = msg.lastSequenceNumberPerActor.indexOf(maxSeqNumber);
            if(getSelf().equals(group.get(maxIndex))){
                SyncMsg syncMsg = new SyncMsg(getSelf());
                broadcastMsg(syncMsg,Arrays.asList(getSelf()));
                currentEpoch++;
            }
            else {
                sendElectionToNextReplica(msg);
            }
        }
        else {
            isElectionInProgress = true;
            System.out.println("\n\nReplica: " + id + "\nHistory:" + history.size());
            msg.lastSequenceNumberPerActor.set(id, history.get(history.size() - 1).sequenceNumber);
            ElectionMsg updatedElectionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor);
            sendElectionToNextReplica(updatedElectionMsg);


        }
        AckMessage ack = new AckMessage(AckType.ELECTION);
        this.sender().tell(ack,getSelf());
    }

    /**
     *
     * @param msg
     */
    private void onSyncMsg(SyncMsg msg) {
        this.isElectionInProgress = false;
        this.coordinator = msg.coordinator;
        this.currentEpoch++;
    }

    private void onIsAliveMsg(IsAliveMsg msg) {
        if(coordinator.equals(getSelf())){
            IsAliveMsg alive = new IsAliveMsg();
            broadcastMsg(alive, Arrays.asList(getSelf()));
        }
        else {
            if(this.isAliveTimer != null) {
                this.isAliveTimer.cancel();
            }
            isAliveTimer = setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), new ElectionMsg(new ArrayList<Integer>(group.size())), false);
        }
    }


    /* -- Actor utils ----------------------------------------------------- */

    private int removeMessageFromBuffer(int value){
        for(int i=0; i<buffer.size(); i++) {
            int bufferedValue = buffer.get(i);
            if (bufferedValue == value) {
                return buffer.remove(i);
            }
        }
        throw new RuntimeException("Buffer is empty! " + buffer.size());
    }

    private void broadCastUpdateRequest(ReplicaMessage msg) {
        ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value);
        buffer.add(msg.value);
        broadcastMsg(update, Arrays.asList(getSelf()));
        ack.put(msg.value, 1);
        AckMessage ackMessage = new AckMessage(msg.value, AckType.UPDATEREQUEST);
        this.sender().tell(ackMessage, getSelf());
    }

    private int incrementAck(int value) {
        int ackNumber = ack.get(value);
        ackNumber++;
        ack.put(value, ackNumber);
        return ackNumber;
    }

    private <T> void broadcastMsg(T msg, List<ActorRef> replicasToExclude) {
        for (ActorRef replica : group) {
            if (!replicasToExclude.contains(replica)) {
                replica.tell(msg, getSelf());
            }
        }
    }

    private <T> Cancellable setTimeout(int millis, ActorRef destination, T msgToSend, boolean isRecurrent) {
        if(isRecurrent){
            return getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.create(millis, TimeUnit.MILLISECONDS),                   // when to start generating messages
                    Duration.create(millis, TimeUnit.MILLISECONDS),                 // how frequently generate them
                    destination,                                                    // destination actor reference
                    msgToSend,                                      // the message to send
                    getContext().system().dispatcher(),                           // system dispatcher
                    getSelf()                                                     // source of the message (myself)
            );
        }
        else {
            return getContext().system().scheduler().scheduleOnce(
                    Duration.create(millis, TimeUnit.MILLISECONDS),                   // when to start generating messages
                    destination,                                                    // destination actor reference
                    msgToSend,                                      // the message to send
                    getContext().system().dispatcher(),                           // system dispatcher
                    getSelf()                                                     // source of the message (myself)
            );
        }
    }

    private void sendElectionToNextReplica(ElectionMsg msg) {
        group.get((id + 1) % group.size()).tell(msg, getSelf());
        if(electionTimer != null) {
            electionTimer.cancel();
        }
        electionTimer = setTimeout(MAX_TIMEOUT * (id + 1), group.get((id + 1) % group.size()), msg, false);
    }

}

