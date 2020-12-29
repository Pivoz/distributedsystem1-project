package it.unitn.ds1.actor.message;

import akka.actor.ActorRef;

import java.io.Serializable;

public class ReplicaMessage implements Serializable {
    public final int epoch;
    public final int sequenceNumber;          // the sequence number of that epoch
    public final int value;          // the sequence number of that epoch
    public final int ackId;

    public ReplicaMessage(int epoch, int sequenceNumber, int value, int ackId ) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
        this.ackId = ackId;
    }

    public ReplicaMessage(int epoch, int sequenceNumber, int value ) {
        this(epoch, sequenceNumber, value, -1);
    }

    public ReplicaMessage(int value) {
        this(-1, -1, value, -1 );
    }
}