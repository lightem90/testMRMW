package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import com.robustMRMW.Node;
import Structures.View;

import java.util.ArrayList;
import java.util.Comparator;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {

    private int quorum;
    private int numberOfNodes;
    private ArrayList<Integer> failureDetector;
    private ArrayList<Node> rep;
    private ArrayList<Node> seemCrd;

    private boolean noCrd = false;
    private boolean imCrd = false;


    public electMasterService(int networkNodes, ArrayList<Integer> nodeFailureDetector, ArrayList<Node> repliesArray ){

        numberOfNodes = networkNodes;
        failureDetector = nodeFailureDetector;
        rep = repliesArray;

        quorum = numberOfNodes/2;

    }

    public void findPossibleMasters(){

        seemCrd = new ArrayList<Node>();

        for (Node n : rep){

            View nodeView = n.getLocalView();


            if ((n.getCm().getFD().calculateActiveNodes() > quorum) &&  nodeView.getIdArray().size() > quorum && isContained(n))
                seemCrd.add(n);
        }

        if (seemCrd.isEmpty() || seemCrd == null) noCrd = true;

    }

    //TODO: method to send the possible master suggestion to all active nodes
    public void proposeMaster(){

        int masterId;
        findPossibleMasters();

        if (!noCrd) {
            //ArrayList<Node> sortedSeemCrd = sortNodeById(seemCrd);
            //masterId = sortedSeemCrd.get(0).getId();
        }

        /*should be implemented in node code
        if (masterId == getId()) imCrd = true;*/

    }



    //TODO: broadcast new elected correct master
    public void electMaster(){

        // if I'm the master send proposed view with new counter and wait for ack, if I'm not wait for message and send ack
        if(imCrd) {

            //inc counter
            //send message

        } else {

            //wait for message

        }


    }



    private boolean isContained (Node l){

        //TODO: implement method to check if the node is contained in the active node list (FD)
        /* int id = l.getId();
            int count = 0;
            ArrayList<Integer> currentPropView = l.getProposedView()

            for (Node k : currentPropView) {

                if (k.getCounter().contains(l)) count++;


            }

        if (count == currentPropView.size) return failureDetector.contains(id);
        else return false;*/


        return true;
    }


    /*
    private ArrayList<Node> sortNodeById(ArrayList<Node> list){

        ArrayList<Node> sortedNodes = (ArrayList)list.clone();
        sortedNodes.sort(new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                if (o1.isMaster() && !o2.isMaster()) {
                    return -1;
                }
                if (!o1.isMaster() && o2.isMaster()) {
                    return 1;
                }
                //return negative value if id2 is bigger than id1
                return o1.getId()- o2.getId();

            }
        });
        return sortedNodes;

    }
    */


}
