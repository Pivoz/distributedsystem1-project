package it.unitn.ds1.actor.message;

import akka.actor.Actor;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class StartMessage implements Serializable {

    private final List<ActorRef> clientList, replicaList;

    public StartMessage(List<ActorRef> clientList, List<ActorRef> replicaList){
        if (clientList == null || replicaList == null)
            throw new NullPointerException();

        this.clientList = Collections.unmodifiableList(clientList);
        this.replicaList = Collections.unmodifiableList(replicaList);
    }

    public List<ActorRef> getClientList(){ return clientList; }
    public List<ActorRef> getReplicaList() { return replicaList; }
}
