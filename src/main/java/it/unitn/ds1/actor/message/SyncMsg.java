package it.unitn.ds1.actor.message;

import akka.actor.ActorRef;

import java.io.Serializable;

public class SyncMsg implements Serializable {
    public final ActorRef coordinator;

    public SyncMsg(ActorRef coordinator) {
        this.coordinator = coordinator;
    }
}
