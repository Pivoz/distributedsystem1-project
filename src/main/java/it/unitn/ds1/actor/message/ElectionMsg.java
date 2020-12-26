package it.unitn.ds1.actor.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ElectionMsg implements Serializable {
    public final List<Integer> lastSequenceNumberPerActor;

    public ElectionMsg(List<Integer> lastSequenceNumberPerActor) {
        this.lastSequenceNumberPerActor = new ArrayList<Integer>(lastSequenceNumberPerActor);
    }

}
