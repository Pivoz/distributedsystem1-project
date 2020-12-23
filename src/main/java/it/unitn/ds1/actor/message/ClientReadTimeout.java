package it.unitn.ds1.actor.message;

import java.io.Serializable;

public class ClientReadTimeout implements Serializable {

    private int replicaPosition;

    public ClientReadTimeout(int replicaPosition) {
        this.replicaPosition = replicaPosition;
    }

    public int getReplicaPosition() {
        return replicaPosition;
    }
}
