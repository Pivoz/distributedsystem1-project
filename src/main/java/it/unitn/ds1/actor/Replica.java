package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.actor.message.*;
import it.unitn.ds1.logger.Logger;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 3000;
    private final int IS_ALIVE_TIMEOUT = 1500;      //ONLY FOR COORDINATOR
    private final int MAX_ELECTION_TIMEOUT = 1500;
    private final int MAX_NETWORK_DELAY = 100;

    private List<ActorRef> group; // the list of peers (the multicast group)
    private ActorRef coordinator;
    private List<Integer> buffer; //list of messages to ack
    private List<ReplicaMessage> history; //history of messages
    private int currentEpoch;
    private int currentSeqNumber;
    private Map<Integer, Integer> ack; //COORDINATOR: keep count of every ack before WRITEOK < value : ack count >
    private final int id;         // ID of the current actor
    private boolean isElectionInProgress;
    private Cancellable isAliveTimer;
    private Map<Integer, Cancellable> updateRequestTimers;
    private Cancellable electionTimer;
    private boolean isCrashed;
    private int ackId; //used by coordinator to keep track of ack

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
        this.ackId = 0;
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
                .match(IsDeadMsg.class,                 this::onIsDeadMsg)
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
        //crashIfReplica(0);
        //crashIfReplica(2);
    }

    /**
     * Create new ReplicaMessage, put into buffer, send to coordinator,
     * start timer if expire COORDINATOR ELECTION
     * @param msg
     */
    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        if(coordinator != null) {
            ReplicaMessage update = new ReplicaMessage(msg.value);
            sendMessage(coordinator, update);

            updateRequestTimers.put(msg.value, setTimeout(MAX_TIMEOUT * group.size() + (id * 500), getSelf(), initializeElectionMessage(), false));
        }
    }

    /**
     * if I'm coordinator, then broadcast UPDATE to others and send ACK to Replica issuing update,
     * else if I'm a simple Replica register new message in buffer, ACK coordinator
     * @param msg
     */
    public void onReplicaMsg(ReplicaMessage msg) {
        if(coordinator != null) {
            if (coordinator.equals(getSelf())) {
                ack.put(ackId, 1);
                broadCastUpdateRequest(msg);
                ackId++;
            } else {
                buffer.add(msg.value);
                if (currentSeqNumber < msg.sequenceNumber) {
                    currentSeqNumber = msg.sequenceNumber;
                }
                AckMessage ackMessage = new AckMessage(msg.ackId, msg.value, AckType.COORDUPDATEREQUEST);
                sendMessage(coordinator, ackMessage);
            }
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
                int ackNumber = incrementAck(msg.id);
                System.out.println("AckMap " + msg.value + ": " + ack.keySet().toString() + " --- " + ack.values().toString());
                if(ackNumber == group.size()/2 + 1) {
                    currentSeqNumber++;
                    WriteOKMessage ok = new WriteOKMessage(currentSeqNumber, msg.value);
                    int bufferedMsg = removeMessageFromBuffer(msg.value);
                    ReplicaMessage replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, msg.value);
                    history.add(replicaMsg);
                    broadcastMsg(ok, Arrays.asList(getSelf()));
                    ack.remove(msg.id);
                    Logger.getInstance().logReplicaUpdate(getSelf().path().name(), currentEpoch, currentSeqNumber, replicaMsg.value);

                    //crashIfCoordinator();
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
        ReplicaMessage replicaMsg;
        if(msg.epoch == -1) {
            replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, msg.value);
            Logger.getInstance().logReplicaUpdate(getSelf().path().name(), currentEpoch, currentSeqNumber, replicaMsg.value);
        }
        else {
            replicaMsg = new ReplicaMessage(msg.epoch, msg.sequenceNumber, msg.value);
            Logger.getInstance().logReplicaUpdate(getSelf().path().name(), msg.epoch, currentSeqNumber, replicaMsg.value);
        }
        history.add(replicaMsg);
    }

    /**
     * If client request a read then answer with last message in history
     * @param msg
     */
    public void onReadRequest(ReadRequestMessage msg) {
        if (!isElectionInProgress) {
            ReadResponseMessage msgToSend;
            Logger logger = Logger.getInstance();
            logger.logReadRequest(this.sender().path().name(), getSelf().path().name());

            if (history.size() > 0) {
                ReplicaMessage currentMsg = this.history.get(this.history.size() - 1);
                msgToSend = new ReadResponseMessage(currentMsg.value);
                logger.log(this.sender().path().name() + " read from Replica" + this.id + " value: " + msgToSend.getValue() + " - Status: " + currentMsg.epoch + ":" + currentMsg.sequenceNumber + " history size: " + history.size());
            } else {
                msgToSend = new ReadResponseMessage(-1);
                logger.log(this.sender().path().name() + " read from Replica" + this.id + " value: -1 (history empty)");
            }
            logger.logReadResult(this.sender().path().name(), msgToSend.getValue());
            sendMessage(this.sender(), msgToSend);


        }
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

        //Remove the crashed coordinator from the replica group
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
                coordinator = getSelf();
                //Recovering history from most updated replica aka the new coordinator
                for(int i=0; i<msg.lastSequenceNumberPerActor.size(); i++){
                    if(group.get(i) != null && !group.get(i).equals(getSelf()) && msg.lastSequenceNumberPerActor.get(id) > msg.lastSequenceNumberPerActor.get(i)){
                        recoverHistory(msg.lastSequenceNumberPerActor.get(i), group.get(i));
                    }
                }
                Logger.getInstance().log("Status: " + msg.lastSequenceNumberPerActor.toString());
                currentEpoch++;
                currentSeqNumber = 0;

                resetTimers();
                isAliveTimer = setIsAliveTimer();
            }
            else {
                sendElectionToNextReplica(msg);
            }
        }
        else {
            Logger.getInstance().signalStartElection("From replica " + id);
            isElectionInProgress = true;
            System.out.println("\n\nReplica: " + id + "\nHistory:" + history.size());
            //System.err.println("----replica"+id+"------------ " + msg.lastSequenceNumberPerActor.size() + " ------------------------");
            if(history.size() > 0) {
                msg.lastSequenceNumberPerActor.set(id, history.get(history.size() - 1).sequenceNumber);
            }
            else{
                msg.lastSequenceNumberPerActor.set(id, 0);
            }
            ElectionMsg updatedElectionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor);
            sendElectionToNextReplica(updatedElectionMsg);
        }

        if (!this.sender().equals(getSelf())) {
            AckMessage ack = new AckMessage(AckType.ELECTION);
            sendMessage(this.sender(), ack);
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
        currentSeqNumber = 0;
        resetTimers();
        //Restart the is-alive timer
        isAliveTimer = setIsAliveTimer();
    }

    private void onIsAliveMsg(IsAliveMsg msg) {
        if(coordinator != null && coordinator.equals(getSelf())){
            IsAliveMsg alive = new IsAliveMsg();
            broadcastMsg(alive, Arrays.asList(getSelf()));
        }
        else {
            if(this.isAliveTimer != null) {
                this.isAliveTimer.cancel();
            }
            isAliveTimer = setTimeout(MAX_TIMEOUT * group.size() + (id * 500), getSelf(), initializeElectionMessage(), false);
        }
    }

    private void onIsDeadMsg(IsDeadMsg msg){
        group.set(msg.id, null);
        ElectionMsg electionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor);
        sendElectionToNextReplica(electionMsg);
    }

    /* -- Actor utils ----------------------------------------------------- */

    private void recoverHistory(int fromSeqNumber, ActorRef dest){
        for(int i = fromSeqNumber; i < history.size(); i++){
            WriteOKMessage ok = new WriteOKMessage(currentEpoch, i, history.get(i).value);
            sendMessage(dest, ok);
        }
    }

    private int removeMessageFromBuffer(int value){
        for(int i=0; i<buffer.size(); i++) {
            int bufferedValue = buffer.get(i);
            if (bufferedValue == value) {
                return buffer.remove(i);
            }
        }
        return -1; //buffer is empty or does not contain that value
    }

    private void broadCastUpdateRequest(ReplicaMessage msg) {
        ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value, ackId);
        buffer.add(msg.value);
        broadcastMsg(update, Arrays.asList(getSelf()));
        AckMessage ackMessage = new AckMessage(msg.value, AckType.UPDATEREQUEST);
        sendMessage(this.sender(), ackMessage);
    }

    private int incrementAck(int id) {
        System.out.println("ACK DEBUG: " + id + " --- " + ack.keySet().toString());
        int ackNumber = 0;
        if(ack.containsKey(id)) {
            ackNumber = ack.get(id);
            ackNumber++;
            ack.put(id, ackNumber);
        }
        return ackNumber;
    }

    /**
     * Send a message to all excluding the specified replicas
     * @param msg message to send
     * @param replicasToExclude list of replicas to exclude
     */
    private void broadcastMsg(Serializable msg, List<ActorRef> replicasToExclude) {
        for (ActorRef replica : group) {
            if (!replicasToExclude.contains(replica)) {
                if(msg instanceof WriteOKMessage && currentSeqNumber % 4 == 0 &&
                        (replica!= null && replica.equals(group.get(5)))){
                    this.crash();
                    return;
                }
                sendMessage(replica, msg);
            }
        }
    }

    /**
     * Simple implementation of Akka Scheduler to set timeout based on if it will be recurrent or not
     * @param millis time before scheduling, used also as interval
     * @param destination destination of message on timer expiration
     * @param msgToSend message to send
     * @param isRecurrent if true set the interval and the timeout, otherwise only the timeout
     * @return
     */
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

    /**
     * Retrieve the next not null replica in the ring and send the Election Message.
     * Then initialize a timer in case of Replica not responding sending an IsDead message
     * @param msg Election message to send
     */
    private void sendElectionToNextReplica(ElectionMsg msg) {

        int index = 0;
        ActorRef nextNonNullReplica;
        do {
            index++;
            System.out.println("Next not null replica: " + id + " + " + index + " size: " + group.size());
            nextNonNullReplica = group.get((id + index) % group.size());

            if (index > group.size()) {
                crash();
                throw new RuntimeException("Replica" + id + ": there aren't any alive replicas");
            }

        } while (nextNonNullReplica == null);

        sendMessage(nextNonNullReplica, msg);
        if(electionTimer != null) {
            electionTimer.cancel();
        }

        IsDeadMsg isDead = new IsDeadMsg((id + index) % group.size(), msg.lastSequenceNumberPerActor);
        electionTimer = setTimeout(MAX_ELECTION_TIMEOUT, getSelf(), isDead, false);
    }

    /**
     * Simulate a network delay before sending a message
     * @param destination the Replica to send the message
     * @param message any type of message defined in the system
     */
    private void sendMessage(ActorRef destination, Serializable message){
        if (this.isCrashed)
            return;

        int networkDelay = (int) (Math.random() * MAX_NETWORK_DELAY);
        try {
            Thread.sleep(networkDelay);
        } catch (InterruptedException ignored) {}

        if (destination != null) {
            System.out.println("Sending message from " + getSelf().path().name() + " to " + destination.path().name() + ": " + message.getClass().getSimpleName());
            if (message.getClass().getSimpleName().equals("AckMessage")){
                AckMessage ack = (AckMessage) message;
                System.out.println("ACK TYPE: " + ack.ackType);
            }
            destination.tell(message, getSelf());
        }
    }

    /**
     * When invoked stops the Replica and cancel all timers of that Replica
     */
    private void crash(){
        //this.isCrashed = true;
        System.err.println("REPLICA " + id + " CRASHED!");

        getContext().stop(getSelf());

        //Cancel all the timers of the crashed replica
        resetTimers();
    }

    private void resetTimers() {
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

    /**
     * Initialize list of seq number with value -1 for each Replica since if crash occurs at very beginning the max seq
     * number is 0
     * @return ElectionMsg with initialized list
     */
    private ElectionMsg initializeElectionMessage(){
        List<Integer> list = new ArrayList<>();
        for (int i=0; i<group.size(); i++)
            list.add(-1);

        return new ElectionMsg(list);
    }

    /**
     * Stops isAlive and updateRequest timers since election is in progress
     */
    private void stopTimersDuringElection(){
        if (isAliveTimer != null)
            isAliveTimer.cancel();

        for (Cancellable timer : updateRequestTimers.values())
            timer.cancel();

        updateRequestTimers = new HashMap<>();
    }

    /**
     * If coordinator set timer to send IsAliveMsg, else set timer to initialize election
     * @return
     */
    private Cancellable setIsAliveTimer(){
        if(coordinator.equals(getSelf())) {
            System.out.println("COORDINATOR IS_ALIVE: " + id);
            return setTimeout(IS_ALIVE_TIMEOUT, getSelf(), new IsAliveMsg(), true);
        }
        else{
            return setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), initializeElectionMessage(), false);
        }
    }

}

