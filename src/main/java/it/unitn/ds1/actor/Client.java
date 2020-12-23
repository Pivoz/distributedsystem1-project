package it.unitn.ds1.actor;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds1.actor.message.SendClientMessage;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class Client extends AbstractActor {

    private int id;
    private final int MESSAGE_INTERVAL_SECONDS = 5;

    private Client(int clientID){
        super();
        this.id = clientID;
    }

    public static Props props(int clientID){
        return Props.create(Client.class, () -> new Client(clientID));
    }

    public int getId(){ return id; }

    @Override
    public void preStart() throws Exception {

        Cancellable timer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.create(1, TimeUnit.SECONDS),                   // when to start generating messages
                Duration.create(MESSAGE_INTERVAL_SECONDS, TimeUnit.SECONDS),  // how frequently generate them
                getSelf(),                                                    // destination actor reference
                new SendClientMessage(),                                      // the message to send
                getContext().system().dispatcher(),                           // system dispatcher
                getSelf()                                                     // source of the message (myself)
        );

    }

    private void onSendClientMessage(SendClientMessage message){
        //Pr(read)=0.8, Pr(update)=0.2
        int dice = (int) (Math.random() * 10);
        if (dice < 8){
            //Read
        }
        else {
            //Update
        }

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SendClientMessage.class, this::onSendClientMessage)
                .build();
    }
}
