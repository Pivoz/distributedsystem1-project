package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.actor.message.*;
import it.unitn.ds1.logger.Logger;
import javafx.util.Pair;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractActor {
    private final int MAX_TIMEOUT = 2000;
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
    private List<Pair<Integer, Cancellable>> updateRequestTimers;
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
        this.updateRequestTimers = new ArrayList<>();
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
     * Invoked when we receive the initial message that contains the clients and replicas groups.
     * Init the List of ActorRef of this Replica and set as coordinator the first in the group.
     * @param message : StartMessage from main
     * */
    public void onStartMessage(StartMessage message){
        this.group = new ArrayList<>(message.getReplicaList());
        //The initial coordinator is the first replica of the list
        coordinator = this.group.get(0);
        currentEpoch++;
        isAliveTimer = setIsAliveTimer();
    }

    /**
     * If no Election in progress:
     * create new ReplicaMessage, put into buffer, send to coordinator,
     * start timer if expire COORDINATOR ELECTION
     * @param msg : ClientUpdateRequestMsg from Client
     */
    private void onClientUpdateRequestMsg(ClientUpdateRequestMsg msg) {
        if(!isElectionInProgress) {
            ReplicaMessage update = new ReplicaMessage(msg.value);
            sendMessage(coordinator, update);
            Pair<Integer, Cancellable> timerForValue = new Pair(msg.value, setTimeout(MAX_TIMEOUT * group.size() + (id * 500), getSelf(), initializeElectionMessage(), false));
            updateRequestTimers.add(timerForValue);
        }
    }

    /**
     * If no Election in progress:
     * if I'm coordinator, then init the ACK list for this updated, put the new value in buffer, broadcast UPDATE to all replicas and ACK the sender of Update Request.
     * Else if I'm a simple Replica, register new message in buffer, update the Seq Number with the one of Coordinator and ACK coordinator
     * @param msg ReplicaMessage from Replica issuing Update or Coordinator if Update in progress
     */
    public void onReplicaMsg(ReplicaMessage msg) {
        if(!isElectionInProgress) {
            AckMessage ackMessage;
            if (coordinator.equals(getSelf())) {
                ack.put(ackId, 1);
                ReplicaMessage update = new ReplicaMessage(currentEpoch, currentSeqNumber, msg.value, ackId);
                ackId++;
                buffer.add(msg.value);
                broadcastMsg(update);
                ackMessage = new AckMessage(msg.value, AckType.UPDATEREQUEST);
                sendMessage(this.sender(), ackMessage);
            } else {
                buffer.add(msg.value);
                if (currentSeqNumber < msg.sequenceNumber) {
                    currentSeqNumber = msg.sequenceNumber;
                }
                ackMessage = new AckMessage(msg.ackId, msg.value, AckType.COORDUPDATEREQUEST);
                sendMessage(coordinator, ackMessage);
            }
        }
    }

    /**
     * Different behaviours based on ACK Type:
     * - UPDATEREQUEST ACk: stop the updateRequestTimer
     * - COORDUPDATEREQUEST ACK: this ACK is delivered only to Coordinator. Increment the number of ACK received for a specific value,
     *   if I reach the QUORUM then remove ACK counter for that value and increment the SeqNumber, updated the history and broadcast a WRITEOK to other Replicas
     * - ELECTION ACK: Stop the ElectionTimer
     * @param msg : AckMessage from Replicas answering after receiving a Message
     */
    public void onAckMsg(AckMessage msg) {
        switch (msg.ackType){
            case UPDATEREQUEST:
                Pair<Integer, Cancellable> timerToRemove = null;
                for(Pair<Integer,Cancellable> timer : updateRequestTimers){
                    if(timerToRemove == null && timer.getKey() == msg.value){
                        timerToRemove = timer;
                    }
                }

                if (timerToRemove != null) {
                    timerToRemove.getValue().cancel();
                    updateRequestTimers.remove(timerToRemove);
                }
                break;
            case COORDUPDATEREQUEST:
                int ackNumber = incrementAck(msg.id);
                if(ackNumber == group.size()/2 + 1) {
                    //TODO: instruction used to crash first replica (at the beginning the coordinator)
                    //if(id == 0) {
                    //    this.crash();
                    //    return;
                    //}
                    ack.remove(msg.id);
                    currentSeqNumber++;
                    WriteOKMessage ok = new WriteOKMessage(currentSeqNumber, msg.value);
                    removeMessageFromBuffer(msg.value);
                    ReplicaMessage replicaMsg = new ReplicaMessage(currentEpoch, msg.sequenceNumber, msg.value);
                    history.add(replicaMsg);
                    broadcastMsg(ok);
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
     * When a Replica receive the WriteOKMessage, update the local SequenceNumber with the one of coordinator and remove the update value from buffer.
     * If the message epoch is -1 then we are in a normal phase and we can create the new ReplicaMessage to put into history with the local epoch value.
     * Otherwise we are in a Sync phase where the history is recovering, so we use the epoch inside the message to rebuild the correct flow of messages.
     * @param msg : WriteOKMessage from coordinator
     */
    public void onWriteOKMsg(WriteOKMessage msg) {
        currentSeqNumber = msg.sequenceNumber;
        removeMessageFromBuffer(msg.value);
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
     * When a client issue a Read to a Replica, if there is no election in progress,
     * send the last value in history or -1 if history empty using ReadResponseMessage
     * @param msg : ReadRequestMessage from client
     */
    public void onReadRequest(ReadRequestMessage msg) {
        if (!isElectionInProgress) {
            ReadResponseMessage msgToSend;
            Logger logger = Logger.getInstance();
            logger.logReadRequest(this.sender().path().name(), getSelf().path().name());
            if (history.size() > 0) {
                ReplicaMessage currentMsg = this.history.get(this.history.size() - 1);
                msgToSend = new ReadResponseMessage(currentMsg.value);
            } else {
                msgToSend = new ReadResponseMessage(-1);
            }
            logger.logReadResult(this.sender().path().name(), msgToSend.getValue());
            sendMessage(this.sender(), msgToSend);
        }
    }

    /**
     * When Replica receive a Election message:
     * If Election in progress, that means we already done a ring cycle, check if max seq number is mine, if yes then initialize the current Replica as coordinator otherwise forward the Election Message.
     * If Election was not in progress, that means the Replica is receiving the Election for the first time, stop UpdateRequest and IsAlive timers to avoid starting new Elections, remove the Coordinator from local group of ActorRef, set the highest SeqNumber for my id and forward the Election to next Replica
     * Finally ACK if I'm not the sender of ElectionMsg (this occurs when new Election start)
     * @param msg : Election message from an active Replica
     */
    private void onElectionMsg(ElectionMsg msg) {
        System.err.println(this.sender().path().name() + " delivered Election msg to " + getSelf().path().name() + ". Remaining TTL: " + msg.TTL);
        if(isElectionInProgress){
            Integer maxSeqNumber = Collections.max(msg.lastSequenceNumberPerActor);
            Integer maxIndex = msg.lastSequenceNumberPerActor.indexOf(maxSeqNumber);
            if(getSelf().equals(group.get(maxIndex))){

                initializeNewCoordinator(msg.lastSequenceNumberPerActor);
                System.err.println("The new Coordinator is: " + getSelf().path().name() + " --- List of Replicas most recent sequence numbers: " + msg.lastSequenceNumberPerActor.toString());
            }
            else {
                ElectionMsg updatedElectionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor, msg.TTL>0 ? msg.TTL-1 : group.size());
                if(msg.TTL == 0) {
                    updatedElectionMsg.lastSequenceNumberPerActor.set(maxIndex, -1);
                }
                sendElectionToNextReplica(updatedElectionMsg);
            }
        }
        else {
            isElectionInProgress = true;
            stopTimersDuringElection();
            //Remove the crashed coordinator from the replica
            removeCrashedCoordinator();
            if(history.size() > 0) {
                msg.lastSequenceNumberPerActor.set(id, history.get(history.size() - 1).sequenceNumber);
            }
            else{
                msg.lastSequenceNumberPerActor.set(id, 0);
            }
            ElectionMsg updatedElectionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor, msg.TTL-1);
            sendElectionToNextReplica(updatedElectionMsg);
        }

        if (!this.sender().equals(getSelf())) {
            AckMessage ack = new AckMessage(AckType.ELECTION);
            sendMessage(this.sender(), ack);
        }
    }

    /**
     * When receiving the SyncMessage the Replica stop the Election phase, set the current coordinator, increment Epoch and reset SequenceNumber.
     * Finally restart the isAlive timer
     * @param msg : SyncMsg from new elected Coordinator
     */
    private void onSyncMsg(SyncMsg msg) {
        this.isElectionInProgress = false;
        if (electionTimer != null)
            electionTimer.cancel();

        this.coordinator = msg.coordinator;
        this.currentEpoch++;
        currentSeqNumber = 0;

        isAliveTimer = setIsAliveTimer();

        System.err.println("SYNC: New Coordinator for " + this.getSelf().path().name() + " is " + coordinator.path().name());
    }

    /**
     * When receiving the isAliveMessage, if current Replica is the coordinator, it need to signal everyone it's alive with a broadcast,
     * otherwise is a common Replica, so it resets the isAliveTimer to prevent an unwanted new Election.
     * Timeout is delayed based on Replica Id and Replicas number to prevent simultaneous elections.
     * @param msg : IsAliveMsg from current coordinator
     */
    private void onIsAliveMsg(IsAliveMsg msg) {
        if(coordinator != null && coordinator.equals(getSelf())){
            IsAliveMsg alive = new IsAliveMsg();
            broadcastMsg(alive);
        }
        else {
            if(this.isAliveTimer != null) {
                this.isAliveTimer.cancel();
            }
            isAliveTimer = setTimeout(MAX_TIMEOUT * group.size() + (id * 500), getSelf(), initializeElectionMessage(), false);
        }
    }

    /**
     * When a Replica receive the IsDeadMsg that means the next Replica in the ring is no more active,
     * so it has to be removed from the local List of Replicas and forward the Election message to the next active Replica
     * @param msg
     */
    private void onIsDeadMsg(IsDeadMsg msg){
        group.set(msg.id, null);
        ElectionMsg electionMsg = new ElectionMsg(msg.lastSequenceNumberPerActor, msg.TTL);
        sendElectionToNextReplica(electionMsg);
    }

    /* -- Actor utils ----------------------------------------------------- */

    /**
     * Send multiple WriteOKMessage to Replica that need to recover lost updates. It is used by new elected coordinator.
     * @param fromSeqNumber : the last sequence number received before the election
     * @param dest : the Replica that needs to recover the updates
     */
    private void recoverHistory(int fromSeqNumber, ActorRef dest){
        for(int i = fromSeqNumber; i < history.size(); i++){
            WriteOKMessage ok = new WriteOKMessage(currentEpoch, i, history.get(i).value);
            sendMessage(dest, ok);
        }
    }

    /**
     * Removes the message waiting for approval from the buffer
     * @param value : the value of update to remove
     * @return the removed value from buffer or -1 if buffer empty (may occur when recovering history)
     */
    private int removeMessageFromBuffer(int value){
        for(int i=0; i<buffer.size(); i++) {
            int bufferedValue = buffer.get(i);
            if (bufferedValue == value) {
                return buffer.remove(i);
            }
        }
        return -1; //buffer is empty or does not contain that value
    }

    /**
     * Increment ack with specified id key
     * @param id : key of ack to increment
     * @return the incremented value or 0 if key not found.
     */
    private int incrementAck(int id) {
        int ackNumber = 0;
        if(ack.containsKey(id)) {
            ackNumber = ack.get(id);
            ackNumber++;
            ack.put(id, ackNumber);
        }
        return ackNumber;
    }

    /**
     * Send a message to all Replicas in the group excluding the sender
     * @param msg : message to send
     */
    private void broadcastMsg(Serializable msg) {
        for (ActorRef replica : group) {
            if (replica != null && !replica.equals(getSelf())) {
                //TODO: this crash
                //if(msg instanceof  WriteOKMessage && group.get(5).equals(replica)){
                //    this.crash();
                //    return;
                //}
                sendMessage(replica, msg);
            }
        }
    }

    /**
     * Simple implementation of Akka Scheduler to set timeout based on if it will be recurrent or not
     * @param millis : time before scheduling, used also as interval
     * @param destination : destination of message on timer expiration
     * @param msgToSend : message to send
     * @param isRecurrent : if true set the interval and the timeout, otherwise only the timeout
     * @return the Cancellable instance of new timer
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
     * In case of no more replica available throw and Exception (this is used only as fallback
     * but should never happen since the assumption guarantees N/2+1 Replicas always active)
     * Finally initialize a timer that in case of Replica not responding sends an IsDead message
     * @param msg Election message to send
     */
    private void sendElectionToNextReplica(ElectionMsg msg) {
        int index = 0;
        ActorRef nextNonNullReplica;
        do {
            index++;
            nextNonNullReplica = group.get((id + index) % group.size());
            if (index > group.size()) {
                crash();
                throw new RuntimeException("Replica" + id + ": there aren't any alive replicas");
            }
        } while (nextNonNullReplica == null);

        sendMessage(nextNonNullReplica, msg);
        //TODO: this crash
        //if((group.get(1) != null && group.get(1).equals(getSelf())) || (group.get(2) != null && group.get(2).equals(getSelf())) && isElectionInProgress){
        //    this.crash();
        //    return;
        //}
        if(electionTimer != null) {
            electionTimer.cancel();
        }
        IsDeadMsg isDead = new IsDeadMsg((id + index) % group.size(), msg.lastSequenceNumberPerActor, msg.TTL);
        electionTimer = setTimeout(MAX_ELECTION_TIMEOUT, getSelf(), isDead, false);
    }

    /**
     * If sender not crashed and destination defined, send a message simulating a network delay
     * @param destination : the Replica to which send the message
     * @param message : any type of message defined in the system
     */
    private void sendMessage(ActorRef destination, Serializable message){
        int networkDelay = (int) (Math.random() * MAX_NETWORK_DELAY);
        try {
            Thread.sleep(networkDelay);
        } catch (InterruptedException ignored) {}
        if (destination != null && !this.isCrashed) {
            destination.tell(message, getSelf());
        }
    }

    /**
     * When invoked cancel all timers and stop current Replica
     */
    private void crash(){
        //Cancel all the timers of the crashed replica
        if (electionTimer != null)
            electionTimer.cancel();

        if (isAliveTimer != null)
            isAliveTimer.cancel();

        for (Pair<Integer, Cancellable> timer : updateRequestTimers) {
            timer.getValue().cancel();
        }
        updateRequestTimers = new ArrayList<>();

        getContext().stop(getSelf());
        System.err.println(getSelf().path().name() + " has CRASHED");
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

        return new ElectionMsg(list, group.size() * 2);
    }

    /**
     * Stops isAlive and updateRequest timers since election is in progress
     */
    private void stopTimersDuringElection(){
        if (isAliveTimer != null)
            isAliveTimer.cancel();

        for (Pair<Integer, Cancellable> timer : updateRequestTimers) {
            timer.getValue().cancel();
        }

        updateRequestTimers = new ArrayList<>();
    }

    /**
     * If coordinator, set timer to send IsAliveMsg, otherwise set timer to initialize election
     * @return Cancellable instance of timer
     */
    private Cancellable setIsAliveTimer(){
        if(coordinator.equals(getSelf())) {
            return setTimeout(IS_ALIVE_TIMEOUT, getSelf(), new IsAliveMsg(), true);
        }
        else{
            return setTimeout(MAX_TIMEOUT * (id + 1), getSelf(), initializeElectionMessage(), false);
        }
    }

    /**
     * Remove the coordinator from current Replica list of nodes since it crashed
     */
    private void removeCrashedCoordinator() {
        for (int i=0; i<group.size(); i++) {
            if (group.get(i) != null && group.get(i).equals(coordinator)) {
                group.set(i, null);
                coordinator = null;
            }
        }
    }

    /**
     * Init current replica as new coordinator, stop election timer, broadcast SYNC with current Replica as coordinator and recover history of other Replicas.
     * Finally setup the isAliveTimer
     * @param lastSequenceNumberPerActor : list of last registered sequence number of each active replica
     */
    private void initializeNewCoordinator(List<Integer> lastSequenceNumberPerActor){

        coordinator = getSelf();
        currentEpoch++;
        currentSeqNumber = 0;

        if (electionTimer != null)
            electionTimer.cancel();

        SyncMsg syncMsg = new SyncMsg(getSelf());
        broadcastMsg(syncMsg);

        //Recovering history from most updated replica aka the new coordinator
        for(int i=0; i<lastSequenceNumberPerActor.size(); i++){
            if(group.get(i) != null
                    && !group.get(i).equals(getSelf())
                    && lastSequenceNumberPerActor.get(id) > lastSequenceNumberPerActor.get(i)
                    && lastSequenceNumberPerActor.get(i) >= 0){
                recoverHistory(lastSequenceNumberPerActor.get(i), group.get(i));
            }
        }

        isElectionInProgress = false;
        isAliveTimer = setIsAliveTimer();
    }

}

