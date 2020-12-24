package it.unitn.ds1.message;

import java.io.Serializable;

public class AckMessage implements Serializable {
    public final int epoch;
    public final int sequenceNumber;
    public final int oldSequenceNumber;

    /**
     * Constructor used by Replica Coordinator
     * @param epoch
     * @param sequenceNumber
     * @param oldSequenceNumber
     */
    public AckMessage(int epoch, int sequenceNumber, int oldSequenceNumber) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
        this.oldSequenceNumber = oldSequenceNumber;
    }

    /**
     * Constructor used by normal Replicas
     * @param epoch
     * @param sequenceNumber
     */
    public AckMessage(int epoch, int sequenceNumber){
        this(epoch, sequenceNumber, -1);
    }
}