package it.unitn.ds1.actor.message;

public class ReplicaMessage {
    public final int epoch;
    public final int sequenceNumber;          // the sequence number of that epoch
    public final int value;          // the sequence number of that epoch
    public final int senderId;

    public ReplicaMessage(int epoch, int sequenceNumber, int value, int senderId ) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
        this.senderId = senderId;
    }

    @Override
    public String toString(){
        return String.format("Message <%d,%d> : %d\n",epoch, sequenceNumber, value);
    }
}