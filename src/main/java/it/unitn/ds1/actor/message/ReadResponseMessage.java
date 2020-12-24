package it.unitn.ds1.actor.message;

import java.io.Serializable;

public class ReadResponseMessage implements Serializable {

    private int value;

    public ReadResponseMessage(int value){
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
