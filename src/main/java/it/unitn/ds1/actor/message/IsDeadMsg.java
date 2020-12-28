package it.unitn.ds1.actor.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IsDeadMsg implements Serializable {
    public final int id;
    public final List<Integer> lastSequenceNumberPerActor;

    public IsDeadMsg(int id, List<Integer> lastSequenceNumberPerActor){
        this.id = id;
        this.lastSequenceNumberPerActor = new ArrayList<Integer>(lastSequenceNumberPerActor);
    }
}
