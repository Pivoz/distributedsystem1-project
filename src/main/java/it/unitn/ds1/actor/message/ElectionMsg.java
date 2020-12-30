package it.unitn.ds1.actor.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ElectionMsg implements Serializable {
    public final List<Integer> lastSequenceNumberPerActor;
    public final int TTL;

    public ElectionMsg(List<Integer> lastSequenceNumberPerActor, int TTL) {
        this.lastSequenceNumberPerActor = new ArrayList<Integer>(lastSequenceNumberPerActor);
        this.TTL = TTL;
    }

}
