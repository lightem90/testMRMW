package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.Message;
import Structures.View;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {


    private Map<Integer,Integer> failureDetector;
    private Map<Integer, Message> rep;
    private ArrayList<Integer> seemCrd;
    private ArrayList<SocketChannel> chan;

    private View view;
    private Settings mSet;
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);


    private boolean noCrd = false;


    /*TODO: Where do we detect current leader is dead? */

    public electMasterService(Settings currentSettings, View localView, ConnectionManager cm, Map<Integer,Integer> nodeFailureDetector){

        failureDetector = nodeFailureDetector;
        view = localView;
        rep = cm.getReplica();
        mSet = currentSettings;
        chan = cm.getConnectedServerChannels();

    }

    public int electMaster(){

        System.out.println("This replica (Election) contains: " + rep.keySet().toString());

        System.out.println("There are: " + mSet.getNumberOfNodes() + " nodes");
        seemCrd = new ArrayList<>(mSet.getNumberOfNodes());
        Set<Integer> idList = failureDetector.keySet();
        System.out.println("Failure detector says these nodes are active: " + idList.toString());

        for (Integer l : idList) {
            System.out.println("Node: " + l);

            //Getting all the information about active node l (Last message with proper view received)
            if (rep.containsKey(l)) {
                Message m = rep.get(l);
                View nodeView = m.getView();
                nodeView.setArrayFromValueString();

                System.out.println("Contained with view: " + nodeView.getValue());

                //isContained checks if l has a quorum for his view and if he is contained in each view of the nodes of his view
                //todo: check if iscontained does is job here or if i need replicas
                if ((nodeView.getIdArray().size() >= mSet.getQuorum()) && isContained(nodeView, l))
                    seemCrd.add(l);
            }
            else {
                //Should be me
                System.out.println("Not in Replica");
                if (failureDetector.size() >= mSet.getQuorum() && isContained(view,mSet.getNodeId()))
                    seemCrd.add(mSet.getNodeId());


            }
        }

        if (seemCrd == null || seemCrd.isEmpty()) noCrd = true;
        else System.out.println("seemCrd contains: " +seemCrd.toString());
        return proposeMaster();

    }

    public int proposeMaster(){

        int masterId = -1;

        if(noCrd){
           /*
           * for every node in my FD that has ME in their FD, count those that have the noCrd set to true
           * if these nodes with noCrd reach a quorum, I propose my FD as View (acting like a leader ??)
           * */


            //Send "noCrd" to everyone letting them know that we have no coordinator
            writeBuffer.clear();
            writeBuffer.put("noCrd".getBytes());

            for (int i = 0; i < chan.size(); i++) {

                try {

                    if(!chan.get(i).isConnectionPending() && chan.get(i).isOpen()) {
                        writeBuffer.flip();
                        while (writeBuffer.hasRemaining()) {
                            System.out.println("Sending noCrd to node: " + chan.get(i).getRemoteAddress());
                            chan.get(i).write(writeBuffer);
                        }
                    }

                } catch (IOException e) {
                    System.out.println("Can't write to node");
                }
            }

            //Start receiving other nodes' "noCrd" and count them until quorum is reached
            int counter=0, i=0;
            while (counter <  mSet.getQuorum()) {

                    readBuffer.clear();
                    String message = "";
                    try {

                        if(!chan.get(i).isConnectionPending() && chan.get(i).isOpen()) {
                            while (chan.get(i).read(readBuffer) > 0) {
                                // flip the buffer to start reading
                                readBuffer.flip();
                                message += Charset.defaultCharset().decode(
                                        readBuffer);
                            }
                        }

                        if (message.equals("noCrd")) {
                            System.out.println("Read noCrd");
                            counter++;
                        }

                    } catch (IOException e) {
                        //if write fails it means that the channel has been closed so we cannot write to it
                        //should remove channel (???)
                    }
                i++;
                if (i >= chan.size()){

                    System.out.println("Network error, cannot read an answer from nodes");
                    break;

                }
            }
            System.out.println("Quorum reached for noCrd");
        }
        else {
            seemCrd.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });
            System.out.println("seemCrd sorted is: " +seemCrd.toString());
            masterId = seemCrd.get(0);
        }

        //TODO: up to now, the noCrd part of the code does not change masterId, but simply leaves it to -1 and arrives here only if quorum is reached. This should not be the final solution
        return handleMasterId(masterId);

    }

    private int handleMasterId(int mId){

        if(mId == -1){
            //noCrd must be true and quorum was reached between servers
            //TODO: should send my view to everyone and let the dicks sort themselves out
        }
        else{
            //real master was elected, WOW
            if(mId == mSet.getNodeId())
                //I'm the leader, decide what to do
                System.out.println("Hi, I am node "+ mId +" and I am the boss");
            else
                //do nothing
                System.out.println("Hi, I am node "+ mSet.getNodeId() +" and I think the boss should be: "+ mId);

        }

    return mId;
    }



    private boolean isContained (View mView, int id){

        System.out.println("Is: " + id + " contained in:" + mView.getValue());
        mView.setArrayFromValueString();
        //for each nodeID in the propView
        for(int i : mView.getIdArray()){

            if (rep.containsKey(i)) {
                Message m = rep.get(i);
                System.out.println("Node " + i + " view contains:" + m.getView().getValue() + " must check for " + id + " presence");
                View v = m.getView();
                v.setArrayFromValueString();

                //Check if mId is in the view (list of active nodes of node i)
                if (!v.getIdArray().contains(id))
                    return false;
            }
        }


        return true;
    }







}



//********************************************* ALGORITH FROM PAPER
/*
DO FOREVER BEGIN:

    //LEADER ELECTION
    LET FDin = failureDetector();                                                                        //Gets data from Failure Detector (or it triggers because leader goes offline)
    LET seemCrd = {                                                                                      //Array of all possible leaders, these are all conditions a node pl should have to be contained in seemCrd
        pl = rep[l].propV.ID.wid ? FD : (|rep[l].propV.set| > [n/2]) ?                                  //Its id should be contained in FD, Its proposed view should contain a majority of nodes
             (|rep[l].FD| > [n/2]) ?                                                                    //Its FD should see a majority
                (pl ? rep[l].propV.set) ?                                                               //Itself must be in the proposedView
                     (pk ? rep[l].propV.set ? pl ? rep[k].FD) ?                                       //Each node of the proposedView should have pl in its FD (according to the last message received)
                           ((rep[l].status = Multicast) ? (rep[l].(view = propV )?crd(l) = l)) ?       //Last status was multicast implies that view = propView and the crd is itself
                                ((rep[l].status = Install) ? crd(l) = l)                                //Last status was Install, the crd is itself

    }
    LET valCrd = {                                                                                      //It is the crd id
         {pl ? seemCrd :
            (?pk ? seemCrd :
                rep[k].propV.ID <= rep[l].propV.ID)};                                                   //If seemCrd.size > 1, the crd is the one with the highest id
    }
    noCrd ? (|valCrd|!= 1);                                                                            //noCrd is set if valCrd in empty (no valid leader)
    crdID ? valCrd;

    //FOLLOWER / LEADER SIDE
     IF (
         (|FD| > [n/2]) ?                                                                               //If FD sees a majority
            ( (|valCrd| != 1) ?                                                                         // AND no leader is elected locally
               (|{pk ? FD : pi ? rep[k].FD ? rep[k].noCrd}| > [n/2]))                                  // AND a majority of nodes didn't elect a leader
             ? ((valCrd = {pi}) ?                                                                       //OR there's a leader
                (FD != propV.set)?                                                                      // AND FD sees a different set from the proposed view
                (|{pk ? FD : rep[k].propV = propV}| > [n/2]))                                           // AND a majority of nodes from FD sees the same proposed view
        )
    THEN
        (status,propV ) ? (Propose, inc(), FDi);                                                        //Set view to propos, with inc counter (looks like a write op) and propose it
    ELSE
        IF (                                                                                            //I'm not proposing
            (valCrd = {pi}) ?                                                                           //If there's a coordinator
                 (? pj ? view.set : rep[j].(view, status, rnd) = (view, status, rnd)) ?                 // AND every node of the current view has the same local view/status/rnd
                    ((status != Multicast) ?                                                            //OR status is not multicast
                        (? pj ? propV.set : rep[j].(propV,status) = (propV,Propose))                    //AND everyone is proposing
            )
        THEN {
            //LEADER SIDE
            IF(status = Multicast)                                                                      //If status is multicast AND I'M THE LEADER (pi)
            THEN {
                 apply(state,msg);                                                                      //Synchronize the state?
                 input ? fetch();                                                                       //Get last multicast message as input
                 foreach pj ? P DO if pj ? view.set THEN msg[j] ? rep[j].input ELSE msg[j] ??;       //Re-apply last valid message or empty to current nodes
                  rnd ? rnd + 1;                                                                        //Update round number
            }
            ELSE IF (status = Propose )                                                                 //If is proposing
            THEN
                (state,status,msg) ? (synchState(rep),Install,synchMsgs(rep));                              //Step to install
             ELSE IF (status = Install )                                                                //If is install
            THEN
                 (view,status,rnd) ? (propV,Multicast, 0);                                                  //Step to multicast
         }
         ELSE {

            IF ( valCrd = {pl} ?                                                                        //If a leader exists
                    l != i ?                                                                            //it's not me
                        ((rep[l].rnd = 0 ?                                                              //round number is 0
                        rnd < rep[l].rnd ?                                                              //round number is lesse tham last msg from leader
                        rep[l].(view != propV ))                                                         //view and propView from leader are different
            THEN {
                //FOLLOWER SIDE
                IF ( rep[l].status = Multicast)                                                         //if leader is multicasting
                THEN {
                    IF ( rep[l].state = ? )
                    THEN
                         rep[l].state ? state                                                           //apply state
                    rep[i] ? rep[l];
                    apply(state,rep[l].msg);
                    input ? fetch();                                                                    //input is last multicast message
                }
                ELSE IF (rep[l].status = Install)
                    THEN rep[i] ? rep[l];
                ELSE IF (rep[l].status = Propose)
                    THEN (status,propV ) ? rep[l].(status,propV );                                      //store last (status/prop)/rep from l
            }
        }
        let m = rep[i];                                                                                 //Consolidate state every PCE round
        IF ((status = Multicast) ?
            rnd(mod PCE) != 0 )
        THEN
          m.state ??;

        LET sendSet =
        (seemCrd ?{pk ? propV.set : valCrd = {pi}}?
            {pk ? FD : noCrd ? (status = Propose)})
        FOREACH pj ? sendSet DO SEND(m);                                                                //Sends state replica each PCE round

Upon message arrival m from pj do rep[j] ? m;                                                           //Store replica from others

*/