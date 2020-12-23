package it.unitn.ds1.actor.message;

import java.io.Serializable;

public class UpdateRequestMessage implements Serializable {

    private int newValue;

    public UpdateRequestMessage(int newValue){
        this.newValue = newValue;
    }

    public int getNewValue() {
        return newValue;
    }
}
