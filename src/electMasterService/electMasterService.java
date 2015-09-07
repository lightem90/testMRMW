package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import Structures.Message;
import com.robustMRMW.Node;
import Structures.View;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {

    private int quorum;
    private int numberOfNodes;
    private Map<Integer,Integer> failureDetector;
    private ArrayList<Message> rep;
    private Map<Integer, Message> repPropView;
    private ArrayList<Integer> seemCrd;

    private boolean noCrd = false;
    private boolean imCrd = false;


    /*TODO: How do we start the leader election routine and how we get this structures? More messages? */
    public electMasterService(int networkNodes, Map<Integer,Integer> nodeFailureDetector, ArrayList<Message> repliesArray, Map<Integer, Message> repliesArrayPropView ){

        numberOfNodes = networkNodes;
        failureDetector = nodeFailureDetector;
        quorum = numberOfNodes/2;

        rep = repliesArray;
        repPropView = repliesArrayPropView;

    }

    public void findPossibleMasters(){

        seemCrd = new ArrayList<>(numberOfNodes);

        for (Message m : rep){

            View nodeView = m.getView();
            nodeView.setArrayFromValueString(); //now I have all the ids in the view array

            Message curr = repPropView.get(m.getSenderId());

            View propView = curr.getView();
            propView.setArrayFromValueString();

            //isContained checks if the sender of the proposed view is contained in each View of the nodes in his proposedView
            if ((nodeView.getIdArray().size() > quorum) &&  propView.getIdArray().size() > quorum && isContained(curr.getSenderId(), propView))
                seemCrd.add(m.getSenderId());
        }

        if (seemCrd.isEmpty() || seemCrd == null) noCrd = true;
        proposeMaster();

    }

    //TODO: method to send the possible master suggestion to all active nodes
    public void proposeMaster(){

        int masterId;
        findPossibleMasters();

        if (!noCrd) {

            seemCrd.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
        }
        masterId = seemCrd.get(0);
        electMaster(masterId);

    }



    //TODO: broadcast new elected correct master and return with the id to node code
    public int electMaster(int mId){

        //TODO: Build ad-hoc message to propose master and wait for quorum (communicate like)
        /*  TODO: Problem -> how to respond to a node that proposes a master? We should check if the proposed master is in the local seemCrd list
                and respond yes/no and then wait again if that node receives a majority and the confirmation that yes he's the master (like a write operation)
                */





    return 0;
    }



    private boolean isContained (int l, View pView){


        //for each nodeID in the propView
        for(int i : pView.getIdArray()){

            Message m = rep.get(i);
            View v = m.getView();
            v.setArrayFromValueString();

            //Check if l is in the view (list of active nodes of node i)
            if (v.getIdArray().contains(l))
                return true;



        }


        return false;
    }







}
