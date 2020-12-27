package it.unitn.ds1.actor.message;

import akka.actor.ActorRef;

import java.io.Serializable;

public class ReplicaMessage implements Serializable {
    public final int epoch;
    public final int sequenceNumber;          // the sequence number of that epoch
    public final int value;          // the sequence number of that epoch

    public ReplicaMessage(int epoch, int sequenceNumber, int value ) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
    }

    public ReplicaMessage(int value) {
        this(-1, -1, value );
    }
}