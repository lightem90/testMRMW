package com.robustMRMW;

import Structures.View;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Matteo on 15/07/2015.
 */
public class FailureDetector {

    private static final int MAXIMUM_HEARTBEAT_VALUE = 50;
    private HashMap<Integer,Integer> activeNodes;
    Node current;



    /*Constructor takes an array of ids and fill the map considering all nodes as active (we may want to change this) */
    public FailureDetector(int[] ids, Node n){

        activeNodes = new HashMap<>(ids.length);

        for (int i : ids){

            activeNodes.put(i,0);

        }
        current = n;

        updateNodeLocalView();

    }

    /* reset counter of node with id n to zero updating all the other ones */
    public void updateFDForNode(int n) {

        boolean flag = false;
        Iterator<Integer> it = activeNodes.keySet().iterator();
        while (it.hasNext()) {
            if (it.next() == n)
                //resetting node counter (replaces old value), if the node was removed for inactivity it will be added again
                activeNodes.put(n, 0);
            else {
            //checking with heartbeat, if it gets too big I remove the node (counting it as inactive)
            int newVal = activeNodes.get(n)+1;

            if (newVal < MAXIMUM_HEARTBEAT_VALUE)
                activeNodes.put(n, newVal);
            else {
                activeNodes.remove(n);
                flag = true;
                 }
            }

        }

        //if a node has gone offline I have to update node view
        if (flag)
            updateNodeLocalView();

    }


    //this method is called when a new node connects after initial setup, in this way I add it to current active nodes
    public void addNewlyConnectedNode(int newNodeId){

        activeNodes.put(newNodeId,0);
        updateNodeLocalView();

    }


    //this method is called each time an id has gone offline and at start (because all connected nodes are considered active)
    private void updateNodeLocalView(){

        //this gets all active nodes ids, builds a new view FIN and sends the information to the node
        Set<Integer> set = activeNodes.keySet();
        View updView = new View (set.toString());
        updView.setArrayFromValueString();
        updView.setStatus(View.Status.FIN);
        current.setLocalView(updView);


    }


    /* Getters and Setters */
    public void setActiveNodes(HashMap<Integer, Integer> activeNodes) {
            this.activeNodes = activeNodes;
    }


    public HashMap<Integer, Integer> getActiveNodes() {
        return activeNodes;
    }


    }
















