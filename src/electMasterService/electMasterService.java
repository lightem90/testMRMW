package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.MachineStateReplica;
import Structures.Message;
import Structures.View;
import com.robustMRMW.Node;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.*;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {

    private final static int INVALID = -1;

    private Map<Integer,Integer> failureDetector;
    private Map<Integer, MachineStateReplica> rep;
    private ArrayList<Integer> seemCrd;

    private View view;
    private View propView;
    private View FD;
    private Settings mSet;
    private Node currentNode;

    public electMasterService(Node n){

        failureDetector = n.getFD().getActiveNodes();
        FD = new View(failureDetector.keySet());          //FD and propView should be the same
        view = n.getLocalView();
        //view.setArrayFromValueString();
        propView  = n.getProposedView();
        //propView.setArrayFromValueString();
        rep = n.getCm().getRep();
        mSet = n.getSettings();
        currentNode = n;

    }

    public int electMaster(){

        //Now it checks if there's a quorum of noCrd or a quorum of nodes with a leader already elected
        int currentSystemLeader = leaderAlreadyElected();
        if (currentSystemLeader > 0)
            return currentSystemLeader;
        //the noCrd quorum case it handled later

        //Not enough informations to do leader election
        if (rep.size() < mSet.getQuorum()-1) {

            currentNode.getFD().setLeader_id(INVALID);
            return INVALID;
        }


        System.out.println("There are: " + mSet.getNumberOfNodes() + " nodes");
        seemCrd = new ArrayList<>(mSet.getNumberOfNodes());
        Set<Integer> idList = failureDetector.keySet();
        int masterId =INVALID;
        System.out.println("Failure detector says these nodes are active: " + idList.toString());

        for (Integer l : idList) {
            System.out.println("Node# " + l);

            //Getting all the information about active node l (Last message with proper view received)
            if (rep.containsKey(l) && l != mSet.getNodeId()) {
                MachineStateReplica nodeReplicatedState = rep.get(l);
                View nodeReplicatedPropView = nodeReplicatedState.getView();
                nodeReplicatedPropView.setArrayFromValueString();

                System.out.println("Contained with view: " + nodeReplicatedPropView.getValue());

                //isContained checks if l has a quorum for his view and if he is contained in each view of the nodes of his view
                if ((nodeReplicatedPropView.getIdArray().size() >= mSet.getQuorum()) && isContained(nodeReplicatedPropView, l))
                    if (nodeReplicatedState.getStatus() == Node.Status.NONE ||
                            nodeReplicatedState.getStatus() == Node.Status.PROPOSE  )
                        seemCrd.add(l);
                    else if (nodeReplicatedState.getStatus() == Node.Status.MULTICAST && nodeReplicatedState.getView().equals(nodeReplicatedState.getPropView()) && nodeReplicatedState.getLeaderId() == nodeReplicatedState.getId())
                        seemCrd.add(l);
                    else if (nodeReplicatedState.getStatus() == Node.Status.INSTALL && nodeReplicatedState.getLeaderId() == nodeReplicatedState.getId())
                        seemCrd.add(l);
            }
            else {
                //Should be the current node
                if (failureDetector.size() >= mSet.getQuorum() && isContained(propView,mSet.getNodeId()) && l == mSet.getNodeId())
                    seemCrd.add(mSet.getNodeId());
                else
                    System.out.println("No information about this node: " + l);
            }
        }

        //Ordering the array by greater id
        if (seemCrd == null || seemCrd.isEmpty()) {
            currentNode.getFD().setLeader_id(INVALID);
            return INVALID;
        }
        else {
            seemCrd.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });
            System.out.println("seemCrd sorted is: " + seemCrd.toString());
            masterId = seemCrd.get(0);
            currentNode.getFD().setLeader_id(masterId);
        }


        if (canPropose(masterId)){
            propView.setStatus(Node.Status.PROPOSE);
            currentNode.getCm().write(propView);
        }
        else {
            if ((masterId == mSet.getNodeId() && checkViewStatusRnd())
                    || (view.getStatus() != Node.Status.MULTICAST && everyOneIsProposing()))
                leaderSideOperations();
            else
                followerSideOperations();
        }

        return  masterId;

    }

//Conditions at line 248
    private boolean canPropose(int mID){

        if ((view.getIdArray().size() >= mSet.getQuorum() && mID == INVALID &&  enoughReplicaWithNoCrd())
                || (mID == mSet.getNodeId() && !(FD.getValue().equals(propView.getValue()))
                    && enoughReplicaWithThisPropView()))
            return true;
        else
            return false;
    }


//Part of these steps are in other parts of the code
    private void leaderSideOperations() {
/*
        //LEADER SIDE
        IF(status = Multicast)                                                                      //If status is multicast AND I'M THE LEADER (pi)
        THEN {
            apply(state,msg);                                                                      //Synchronize the state?
            input <- fetch();                                                                       //Get last multicast message as input
            foreach pj in P
            DO if pj belongs to view.set
            THEN msg[j] <- rep[j].input
            ELSE msg[j] <- empty;                                                           //Re-apply last valid message or empty to current nodes
            rnd = rnd + 1;                                                                        //Update round number
        }ELSE
        IF (status = Propose )                                                                               //If is proposing
        THEN
                (state,status,msg) <- (synchState(rep),Install,synchMsgs(rep));                              //Step to install
        ELSE IF (status = Install )                                                                       //If is install
        THEN
                (view,status,rnd) <- (propV,Multicast, 0);                                                  //Step to multicast


*/

    }



    private void followerSideOperations() {

/*
        //FOLLOWER SIDE
        IF ( rep[l].status = Multicast)                                                         //if leader is multicasting
        THEN {
            IF ( rep[l].state = empty )
            THEN
            rep[l].state && state                                                           //apply state
            rep[i] <- rep[l];
            apply(state,rep[l].msg);
            input <- fetch();                                                                    //input is last multicast message
        }
        ELSE IF (rep[l].status = Install)
        THEN rep[i] <- rep[l];
        ELSE IF (rep[l].status = Propose)
        THEN (status,propV ) <- rep[l].(status,propV );                                      //store last (status/prop)/rep from l


*/

    }




//Utilities

    //Check if the argument id is contained in each node view of the view passed as argument
    private boolean isContained (View mView, int id){

        if (rep.isEmpty())
            return  false;
        System.out.println("Is: " + id + " contained in:" + mView.getValue());
        //mView.setArrayFromValueString();
        // Return if the proposed view doesn't contain myself
        if (!mView.getIdArray().contains(mSet.getNodeId()))
            return false;
        //for each nodeID in the propView
        for(int i : mView.getIdArray()){

            if (rep.containsKey(i)) {
                MachineStateReplica nodeReplica = rep.get(i);
                System.out.println("Node " + i + " view contains:" + nodeReplica.getPropView().getValue() + " must check for " + id + " presence");
                View nodePropView = nodeReplica.getPropView();
                nodePropView.setArrayFromValueString();

                //Check if mId is in the view (list of active nodes of node i)
                if (!nodePropView.getIdArray().contains(id))
                    return false;
            }
        }

        return true;
    }

//Counts how many nodes of this view share this very same propView (FD)
    private boolean enoughReplicaWithThisPropView()
    {
        int counter = 0;
        for(int i : FD.getIdArray()){

            if (rep.containsKey(i)) {
                if (rep.get(i).getPropView().getValue().equals(propView.getValue())) {
                    if (++counter >= mSet.getQuorum())
                        return true;

                }
            }
        }
        return false;
    }

//Counts how many replicas has noCrd set
    private boolean enoughReplicaWithNoCrd()
    {
        int counter = 0;
        for(int i : FD.getIdArray()){

            if (rep.containsKey(i)) {
                if (rep.get(i).getLeaderId() == INVALID)
                    if (++counter >= mSet.getQuorum())
                        return  true;
            }
        }

        return false;
    }
//checks if every node in the view has the same rnd status and view as current node
    private boolean checkViewStatusRnd(){

        for (int i : view.getIdArray())
        {
            if (rep.containsKey(i))
            {
                MachineStateReplica tmp = rep.get(i);
                if (!(tmp.getStatus() == currentNode.getLocalView().getStatus()
                        && tmp.getRnd() == currentNode.getCm().getRnd()
                            && view.getValue().equals(tmp.getView().getValue())))
                    return false;
            }
        }
        return true;

    }

//Checks if every node in the prop view is proposing
    private boolean everyOneIsProposing(){

        int counter = 0;
        for(int i : propView.getIdArray()){

            if (rep.containsKey(i) && i != mSet.getNodeId()) {
                if (rep.get(i).getPropView().getValue().equals(propView.getValue())
                        && rep.get(i).getPropView().getStatus() == Node.Status.PROPOSE) {
                    if (++counter >= mSet.getQuorum())
                        return true;

                }
            }
        }
        return false;


    }

    //checks if all the replicas have a leader already elected
    private int leaderAlreadyElected()
    {

        int leaderId = INVALID;
        HashMap<Integer,Integer> counter = new HashMap<>();
        for (int i : propView.getIdArray()) {
            if (rep.containsKey(i)) {
                MachineStateReplica tmp = rep.get(i);
                //for each replica it puts their leader id into a counter. In this way it knows how many nodes share the same leader
                if (counter.containsKey((tmp.getLeaderId()))){
                    int c = counter.get(tmp.getLeaderId());
                    counter.put(tmp.getLeaderId(),++c);
                }
                else
                    counter.put(tmp.getLeaderId(),0);
            }
        }

        leaderId = findMaxCounter(counter);

        //returns INVALID if there's no node with a majority
        return leaderId;
    }

    //find the max leader that has a quorum
private int findMaxCounter(HashMap<Integer,Integer> counter){

    Set<Integer> keys = counter.keySet();
    int counterValueForLeader = INVALID;
    int leaderID = Integer.MIN_VALUE;

    for (int i : keys)
    {
        int tmp = counter.get(i);
        if (tmp > counterValueForLeader && tmp >= mSet.getQuorum()) {
            //update max, only one can have a majority tho
            counterValueForLeader = tmp;
            leaderID = i;
        }


    }
    //returns INVALID if there's no node with a majority
    return leaderID;


}





}



//********************************************* ALGORITHM FROM PAPER
/*
DO FOREVER BEGIN:

    //LEADER ELECTION
    LET FDin = failureDetector();                                                                        //Gets data from Failure Detector (or it triggers because leader goes offline)
    LET seemCrd = {                                                                                      //Array of all possible leaders, these are all conditions a node pl should have to be contained in seemCrd
        pl = rep[l].propV.ID.wid belongs to FD : (|rep[l].propV.set| > [n/2]) &&                                  //Its id should be contained in FD, Its proposed view should contain a majority of nodes
             (|rep[l].FD| > [n/2]) &&                                                                    //Its FD should see a majority
                (pl belongs to rep[l].propV.set) &&                                                               //Itself must be in the proposedView
                     (pk belonging to rep[l].propV.set implies pl belonging to rep[k].FD) &&                                       //Each node of the proposedView should have pl in its FD (according to the last message received)
                           ((rep[l].status = Multicast) implies (rep[l].(view = propV )&& crd(l) = l)) &&       //Last status was multicast implies that view = propView and the crd is itself
                                ((rep[l].status = Install) && crd(l) = l)                                //Last status was Install, the crd is itself

    }
    LET valCrd = {                                                                                      //It is the crd id
         {pl in seemCrd :
            (for each pk in seemCrd :
                rep[k].propV.ID <= rep[l].propV.ID)};                                                   //If seemCrd.size > 1, the crd is the one with the highest id
    }
    noCrd = (|valCrd|!= 1);                                                                            //noCrd is set if valCrd in empty (no valid leader)
    crdID = valCrd;

    //FOLLOWER / LEADER SIDE
     IF (
         (|FD| > [n/2]) &&                                                                               //If FD sees a majority
            ( (|valCrd| != 1) &&                                                                         // AND no leader is elected locally
               (|{pk belonging to FD : pi belonging to rep[k].FD && rep[k].noCrd}| > [n/2]))              // AND a majority of nodes didn't elect a leader
             || ((valCrd = {pi}) &&                                                                       //OR i'm the leader
                (FD != propV.set)&&                                                                      // AND FD sees a different set from the proposed view
                (|{pk belonging to FD : rep[k].propV = propV}| > [n/2]))                                 // AND a majority of nodes from FD sees the same proposed view
        )
    THEN
        (status,propV ) = (Propose, inc(), FDi);                                                        //Set view to propos, with inc counter (looks like a write op) and propose it
    ELSE

        IF (                                                                                            //I'm not proposing
            (valCrd = {pi}) &&                                                                           //If i'm the coordinator
                 (for each pj in view.set : rep[j].(view, status, rnd) = (view, status, rnd)) ||                 // AND every node of the current view has my same local view/status/rnd
                    ((status != Multicast) &&                                                            //OR status is not multicast
                        (for each pj in propV.set : rep[j].(propV,status) = (propV,Propose))                    //AND everyone is proposing as I am
            )
        THEN {
            //LEADER SIDE
            IF(status = Multicast)                                                                      //If status is multicast AND I'M THE LEADER (pi)
            THEN {
                 apply(state,msg);                                                                      //Synchronize the state?
                 input <- fetch();                                                                       //Get last multicast message as input
                 foreach pj in P
                    DO if pj belongs to view.set
                        THEN msg[j] <- rep[j].input
                        ELSE msg[j] <- empty;                                                           //Re-apply last valid message or empty to current nodes
                  rnd = rnd + 1;                                                                        //Update round number
            }ELSE
            IF (status = Propose )                                                                               //If is proposing
                THEN
                    (state,status,msg) <- (synchState(rep),Install,synchMsgs(rep));                              //Step to install
                ELSE IF (status = Install )                                                                       //If is install
                    THEN
                     (view,status,rnd) <- (propV,Multicast, 0);                                                  //Step to multicast
             }
         ELSE {

            IF ( valCrd = {pl} &&                                                                        //If a leader exists
                    l != i &&                                                                            //it's not me
                        ((rep[l].rnd = 0 ||                                                              //round number is 0
                        rnd < rep[l].rnd ||                                                              //round number is lesse tham last msg from leader
                        rep[l].(view != propV ))                                                         //view and propView from leader are different
            THEN {
                //FOLLOWER SIDE
                IF ( rep[l].status = Multicast)                                                         //if leader is multicasting
                THEN {
                    IF ( rep[l].state = empty )
                    THEN
                         rep[l].state && state                                                           //apply state
                    rep[i] <- rep[l];
                    apply(state,rep[l].msg);
                    input <- fetch();                                                                    //input is last multicast message
                }
                ELSE IF (rep[l].status = Install)
                    THEN rep[i] <- rep[l];
                ELSE IF (rep[l].status = Propose)
                    THEN (status,propV ) <- rep[l].(status,propV );                                      //store last (status/prop)/rep from l
            }
        }
        let m = rep[i];                                                                                 //Consolidate state every PCE round
        IF ((status = Multicast) &&
            rnd(mod PCE) != 0 )
        THEN
          m.state <- empty;

        LET sendSet =
        (seemCrd union{pk belonging to propV.set : valCrd = {pi}} union
            {pk belonging to FD : noCrd || (status = Propose)})
        FOREACH pj belonging to sendSet
            DO SEND(m);                                                                //Sends state replica each PCE round

Upon message arrival m from pj do rep[j] ? m;                                                           //Store replica from others

*/