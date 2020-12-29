package it.unitn.ds1.actor.message;

import java.io.Serializable;

public class WriteOKMessage implements Serializable {
    public final int epoch;
    public final int sequenceNumber;
    public final int value;

    public WriteOKMessage(int epoch, int sequenceNumber, int value) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
    }

    public WriteOKMessage(int sequenceNumber, int value) {
        this(-1, sequenceNumber, value);
    }
}
