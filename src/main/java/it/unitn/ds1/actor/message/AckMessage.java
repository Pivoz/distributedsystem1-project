package it.unitn.ds1.actor.message;

import java.io.Serializable;


public class AckMessage implements Serializable {
    public final int id;
    public final int value;
    public final int sequenceNumber;
    public final AckType ackType;


    public AckMessage(int id, int value, int sequenceNumber, AckType ackType){
        this.id = id;
        this.value = value;
        this.sequenceNumber = sequenceNumber;
        this.ackType = ackType;
    }

    /**
     * Constructor used by Replica Coordinator
     * @param value
     * @param ackType
     */
    public AckMessage(int id, int value, AckType ackType) {
        this(id, value, -1, ackType);
    }
    public AckMessage( int value, AckType ackType) {
        this(-1, value, -1, ackType);
    }

    /**
     * Used during Election
     * @param ackType
     */
    public AckMessage(AckType ackType){
        this(-1, -1, -1, ackType);
    }
}