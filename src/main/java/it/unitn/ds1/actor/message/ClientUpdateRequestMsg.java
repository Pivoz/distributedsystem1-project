package it.unitn.ds1.actor.message;

import java.io.Serializable;

public class ClientUpdateRequestMsg implements Serializable {
    public final int value;          // the value of client update

    public ClientUpdateRequestMsg(int value) {
        this.value = value;
    }
}
