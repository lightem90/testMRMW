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


    private static final int MAXIMUM_HEARTBEAT_VALUE_MULT = 100;
    private HashMap<Integer,Integer> activeNodes;
    Node current;
    int leader_id;
    long heartbeat;



    /*Constructor takes an array of ids and fill the map considering all nodes as in-active (we may want to change this) */
    public FailureDetector(ArrayList<Integer>ids, Node n){

        activeNodes = new HashMap<>(ids.size());
        activeNodes.put(n.getSettings().getNodeId(), 0);
        current = n;
        leader_id = -1;
        //Heartbeat value becomes a function of the total number of nodes
        heartbeat = MAXIMUM_HEARTBEAT_VALUE_MULT * ids.size();
        System.out.println("Failure detector for " + ids.size() + " nodes->" + ids.toString() + "\nHearthbeat maximum value:" + heartbeat);

        updateNodeProposedView();

    }

    /* reset counter of node with id n to zero updating all the other ones (the i) */
    public void updateFDForNode(int n) {

        //Discarding -1
        if (n == -1)
            return;

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
            //in the active nodes I consider myself as well (needed in leader election for views comparison)
            if (!(i == n) && !(i==current.getSettings().getNodeId())){
            //checking with heartbeat, if it gets too big I remove the node (counting it as inactive)
            int newVal = activeNodes.get(i)+1;

            if (newVal < heartbeat)
                activeNodes.put(i, newVal);
            else {
                it.remove();
                activeNodes.remove(i);
                if (i == leader_id){
                    System.out.println("Leader has gone offline");
                    leader_id = -1;
                }

                    flag = true;
                 }
            }

        }


        //if a node has gone offline I have to update node view
        if (flag)
            updateNodeProposedView();

    }


    //this method is called when a new node connects after initial setup, in this way I add it to current active nodes
    public void addNewlyConnectedNode(int newNodeId){

        activeNodes.put(newNodeId,0);
        updateNodeProposedView();

    }


    //this method is called each time an id has gone offline, at start (because all connected nodes are considered active) and in general each time we signal a change to the node
    private void updateNodeProposedView(){

        //this gets all active nodes ids, builds a new view FIN and sends the information to the node
        System.out.println("Updating view from FD");
        Set<Integer> set = activeNodes.keySet();
        System.out.println("View if FD: " + set.toString());
        View updView = new View (set);
        System.out.println("Nodes seen by FD after update: " + updView.getValue());
        updView.setStatus(current.getStatus());
        current.setProposedView(updView);

    }


    /* Getters and Setters */
    public void setActiveNodes(HashMap<Integer, Integer> activeNodes) {
            this.activeNodes = activeNodes;
    }


    public HashMap<Integer, Integer> getActiveNodes() {
        return activeNodes;
    }


    public int getLeader_id() {
        return leader_id;
    }

    public void setLeader_id(int leader_id) {
        this.leader_id = leader_id;
    }


    }
















