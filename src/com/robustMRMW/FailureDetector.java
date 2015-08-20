package com.robustMRMW;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by Matteo on 15/07/2015.
 */
public class FailureDetector {

    private static final int MAXIMUM_HEARTBEAT_VALUE = 50;
    private HashMap<Integer,Integer> activeNodes;



    /*Constructor takes an array of ids and fill the map considering all nodes as active (we may want to change this) */
    public FailureDetector(int[] ids){

        activeNodes = new HashMap<>(ids.length);

        for (int i : ids){

            activeNodes.put(i,0);

        }



    }

    /* reset counter of node with id n to zero updating all the other ones */
    public void updateFDForNode(int n) {

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
                else activeNodes.remove(n);

            }


        }
    }


    /* Getters and Setters */
    public void setActiveNodes(HashMap<Integer, Integer> activeNodes) {
            this.activeNodes = activeNodes;
    }


    public HashMap<Integer, Integer> getActiveNodes() {
        return activeNodes;
    }


    }
















