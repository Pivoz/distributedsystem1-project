package it.unitn.ds1.message;

import java.io.Serializable;

public class WriteOKMessage implements Serializable {
    public final int epoch;
    public final int sequenceNumber;

    public WriteOKMessage(int epoch, int sequenceNumber) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
    }
}
