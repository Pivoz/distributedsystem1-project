package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.actor.message.*;
import it.unitn.ds1.logger.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 3000; //3 seconds
    private final int IS_ALIVE_TIMEOUT = 1500; //ONLY FOR COORDINATOR
    private final int MAX_ELECTION_TIMEOUT = 1000; //1 seconds

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
    private boolean isCrashed;

    /* -- Actor constructor --------------------------------------------------- */
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
        this.isCrashed = false;
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
        this.group = new ArrayList<>(message.getReplicaList());
        //The initial coordinator is the first replica of the list
        coordinator = this.group.get(0);
        currentEpoch++;

        isAliveTimer = setIsAliveTimer();
    }

    /**
     * Create new ReplicaMessage, put into buffer, send to coordinator,
     * start timer if expire COORDINATOR ELECTION
     * @param msg
     */
    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        if(!isElectionInProgress) {
            ReplicaMessage update = new ReplicaMessage(msg.value);
            sendMessage(coordinator, update, getSelf());

            updateRequestTimers.put(msg.value, setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), initializeElectionMessage(), false));

            //System.err.println("DEBUG replica "+id+": " + getCapacity(electionMsg.lastSequenceNumberPerActor));
        }
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
            sendMessage(coordinator, ackMessage, getSelf());
        }
    }

    /**
     * Stop the timeout corresponding to epoch e and sequence number i, then put message <e,i> from buffer to history,
     * if I'm coordinator update the ack counter and check QUORUM if counter is greater or equal to N/2 (+ 1 that is the coordinator) then send WRITEOK
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
                if(ackNumber == group.size()/2) {
                    currentSeqNumber++;
                    WriteOKMessage ok = new WriteOKMessage(currentSeqNumber, msg.value);
                    int bufferedMsg = removeMessageFromBuffer(msg.value);
                    ReplicaMessage replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, msg.value);
                    history.add(replicaMsg);
                    broadcastMsg(ok, Arrays.asList(getSelf()));
                    Logger.getInstance().logReplicaUpdate(getSelf().path().name(), currentEpoch, currentSeqNumber, replicaMsg.value);

                    crashIfCoordinator();
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

        if (history.size() > 0) {
            ReplicaMessage currentMsg = this.history.get(this.history.size() - 1);
            msgToSend = new ReadResponseMessage(currentMsg.value);
        } else {
            msgToSend = new ReadResponseMessage(-1);
        }
        sendMessage(this.sender(), msgToSend, getSelf());
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
        System.err.println("SENDER ELECTION = " + this.sender());

        //Remove the crashed coodinator from the replica group
        for (int i=0; i<group.size(); i++) {
            if (group.get(i) != null && group.get(i).equals(coordinator)) {
                group.set(i, null);
                coordinator = null;
            }
        }

        stopTimersDuringElection();

        if(isElectionInProgress){
            Integer maxSeqNumber = Collections.max(msg.lastSequenceNumberPerActor);
            Integer maxIndex = msg.lastSequenceNumberPerActor.indexOf(maxSeqNumber);
            if(getSelf().equals(group.get(maxIndex))){
                SyncMsg syncMsg = new SyncMsg(getSelf());
                broadcastMsg(syncMsg,Arrays.asList(getSelf()));
                currentEpoch++;

                coordinator = getSelf();
                isAliveTimer = setIsAliveTimer();
            }
            else {
                sendElectionToNextReplica(msg);
            }
        }
        else {
            isElectionInProgress = true;
            //System.out.println("\n\nReplica: " + id + "\nHistory:" + history.size());
            //System.err.println("----replica"+id+"------------ " + msg.lastSequenceNumberPerActor.size() + " ------------------------");
            msg.lastSequenceNumberPerActor.set(id, history.get(history.size() - 1).sequenceNumber);
            ElectionMsg updatedElectionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor);
            sendElectionToNextReplica(updatedElectionMsg);


        }

        if (!this.sender().equals(getSelf())) {
            AckMessage ack = new AckMessage(AckType.ELECTION);
            sendMessage(this.sender(), ack, getSelf());
        }
    }

    /**
     *
     * @param msg
     */
    private void onSyncMsg(SyncMsg msg) {
        this.isElectionInProgress = false;
        this.coordinator = msg.coordinator;

        System.err.println("NEW COORDINATOR FROM REPLICA" + id + " is " + coordinator.path().name());
        this.currentEpoch++;

        //Restart the is-alive timer
        isAliveTimer = setIsAliveTimer();
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
            isAliveTimer = setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), initializeElectionMessage(), false);
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
        AckMessage ackMessage = new AckMessage(msg.value, AckType.UPDATEREQUEST);
        sendMessage(this.sender(), ackMessage, getSelf());
    }

    private int incrementAck(int value) {
        System.out.println("ACK DEBUG: " + value + " --- " + ack.keySet().toString());
        int ackNumber;
        if(ack.containsKey(value)) {
            ackNumber = ack.get(value);
            ackNumber++;
            ack.put(value, ackNumber);
        }
        else{
            ackNumber = 1;
            ack.put(value, ackNumber);
        }
        return ackNumber;
    }

    private void broadcastMsg(Serializable msg, List<ActorRef> replicasToExclude) {
        for (ActorRef replica : group) {
            if (!replicasToExclude.contains(replica)) {
                sendMessage(replica, msg, getSelf());
            }
        }
    }

    private Cancellable setTimeout(int millis, ActorRef destination, Serializable msgToSend, boolean isRecurrent) {
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
        sendMessage(group.get((id + 1) % group.size()), msg, getSelf());
        if(electionTimer != null) {
            electionTimer.cancel();
        }
        electionTimer = setTimeout(MAX_ELECTION_TIMEOUT, group.get((id + 2) % group.size()), msg, false);
    }

    private void sendMessage(ActorRef destination, Serializable message, ActorRef sender){
        if (this.isCrashed)
            return;

        //TODO: implement network time delay

        //System.out.println("SENDMESSAGE: " + isCrashed + " " + destination.path().name() + " " + sender.path().name());
        if (destination != null) {
            System.out.println("Sending message from " + sender.path().name() + " to " + destination.path().name() + ": " + message.getClass().getSimpleName());
            if (message.getClass().getSimpleName().equals("AckMessage")){
                AckMessage ack = (AckMessage) message;
                System.out.println("ACK TYPE: " + ack.ackType);
            }
            destination.tell(message, sender);
        }
    }

    private void crash(){
        //this.isCrashed = true;
        System.err.println("REPLICA " + id + " CRASHED!");

        getContext().stop(getSelf());

        //Cancel all the timers of the crashed replica
        if (electionTimer != null)
            electionTimer.cancel();

        if (isAliveTimer != null)
            isAliveTimer.cancel();

        for (Cancellable timer : updateRequestTimers.values())
            timer.cancel();
    }

    private void crashIfCoordinator(){
        if (coordinator.equals(getSelf()))
            this.crash();
    }

    private void crashIfReplica(int replicaID){
        if (this.id == replicaID)
            this.crash();
    }

    private ElectionMsg initializeElectionMessage(){
        List<Integer> list = new ArrayList<>();
        for (int i=0; i<group.size(); i++)
            list.add(0);

        return new ElectionMsg(list);
    }

    private void stopTimersDuringElection(){
        if (isAliveTimer != null)
            isAliveTimer.cancel();

        for (Cancellable timer : updateRequestTimers.values())
            timer.cancel();

        updateRequestTimers = new HashMap<>();
    }

    private Cancellable setIsAliveTimer(){
        if(coordinator.equals(getSelf())) {
            return setTimeout(IS_ALIVE_TIMEOUT, getSelf(), new IsAliveMsg(), true);
        }
        else{
            return setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), initializeElectionMessage(), false);
        }
    }

}

