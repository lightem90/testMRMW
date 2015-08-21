package com.robustMRMW;

import Structures.View;

import java.util.ArrayList;
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



    /*Constructor takes an array of ids and fill the map considering all nodes as in-active (we may want to change this) */
    public FailureDetector(ArrayList<Integer>ids, Node n){

        activeNodes = new HashMap<>(ids.size());

        //at start we consider all nodes offline so no need to do this, because it will set a wrong initial view
        /*for (int i : ids){

            activeNodes.put(i,MAXIMUM_HEARTBEAT_VALUE);

        }
        */
        current = n;

        updateNodeLocalView();

    }

    /* reset counter of node with id n to zero updating all the other ones (the i) */
    public void updateFDForNode(int n) {

        boolean flag = false;

        //Signaling a modification to activeNode as occurred (because n is not in the activeNode list)
        if (!activeNodes.containsKey(n)) {
            flag = true;
        }

        //resetting node counter (replaces old value), if the node was removed for inactivity it will be added again
        activeNodes.put(n, 0);


        Iterator<Integer> it = activeNodes.keySet().iterator();

        while (it.hasNext()) {
            int i = it.next();
            if (!(i == n)){
            //checking with heartbeat, if it gets too big I remove the node (counting it as inactive)
            int newVal = activeNodes.get(i)+1;

            if (newVal < MAXIMUM_HEARTBEAT_VALUE)
                activeNodes.put(i, newVal);
            else {
                it.remove();
                activeNodes.remove(i);
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
        View updView = new View (set);
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
















