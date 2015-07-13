package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import com.robustMRMW.Node;
import com.robustMRMW.View;

import java.util.ArrayList;
import java.util.Comparator;


public class electMasterService {

    private int quorum;
    private int numberOfNodes;
    private ArrayList<Integer> failureDetector;
    private ArrayList<Node> rep;
    private ArrayList<Node> seemCrd;

    private boolean noCrd = false;


    public electMasterService(int networkNodes, ArrayList<Integer> nodeFailureDetector, ArrayList<Node> repliesArray ){

        numberOfNodes = networkNodes;
        failureDetector = nodeFailureDetector;
        rep = repliesArray;

        quorum = numberOfNodes/2+1;

    }

    //TODO: method to send the possible master suggestion to all active nodes
    public void proposeMaster(){

        int masterId;
        findPossibleMasters();

        if (!noCrd) {
            ArrayList<Node> sortedSeemCrd = sortNodeById(seemCrd);
            masterId = sortedSeemCrd.get(0).getId();
        }

        //send proposed and wait for answer



    }

    //TODO: broadcast new elected correct master
    public void electMaster(){





    }

    public void findPossibleMasters(){

        seemCrd = new ArrayList<Node>();

        for (Node n : rep){

            View nodeView = n.getLocalView();


            if ((n.getCounter().size() > quorum) &&  nodeView.getIdArray().size() > quorum && isContained(n))
                seemCrd.add(n);
        }

        if (seemCrd.isEmpty() || seemCrd == null) noCrd = true;

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
        else return false;

        */


        return true;
    }


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
                return o1.getId()- o2.getId();

            }
        });
        return sortedNodes;

    }

}
