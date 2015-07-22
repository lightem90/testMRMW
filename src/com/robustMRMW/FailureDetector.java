package com.robustMRMW;

import com.sun.javafx.image.IntPixelGetter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Matteo on 15/07/2015.
 */
public class FailureDetector {

    private static final int PRESENT = 1;
    private static final int EMPTY = -1;
    private static final int MAXIMUM_HEARTBEAT_VALUE = 50;

    private int[] activeNodes;
    private int[] offlineNodes;
    private int nNodes;


    /* Constructor: supposed nodes array containing the network nodes id in growing number from index 1 to n number of nodes */
    // We can do this because we simplify the id to 1 -> n
    // We identify an invalid position with -1 (if for example node 1 (position 0 is offline, the value at index 0 of activeNodes must be -1
    public FailureDetector(int numberOfNodes){

        nNodes = numberOfNodes;
        activeNodes = new int[numberOfNodes+1];
        offlineNodes =  new int[numberOfNodes+1];

        for (int i = 1; i<=nNodes;i++){
            //set all nodes as active
            activeNodes[i]= PRESENT;
            offlineNodes[i] = EMPTY;

        }



    }

    /* reset counter of node with id n to zero updating all the other ones */
    private void updateFDForNode(int n){

        //if it was present in the offlineNodes I should invalidate his position
        if (offlineNodes[n] > 0) offlineNodes[n]=EMPTY;

        for (int i = 1;i<=nNodes;i++){

            //updating all active nodes, we don't care of offline nodes they are alreay out
            if (activeNodes[i] > 0)
                if (activeNodes[i] >= MAXIMUM_HEARTBEAT_VALUE) {
                    //move to offline nodes
                    activeNodes[i] = EMPTY;
                    offlineNodes[i]=PRESENT;
                }
                //else threshold is not reach update
                else activeNodes[i]++;


        }
        //resetting hearthbeat for n
        activeNodes[n]=PRESENT;


    }


    public ArrayList<Integer> calculateActiveNodes(){

        ArrayList<Integer> ret = new ArrayList<Integer>(nNodes);


        for (int i = 1;i<=nNodes;i++){

            //adding node ID if it's a valid position
            if (activeNodes[i] > 0)
                // and his hearthbeat is smaller that threshold
                if (activeNodes[i] < MAXIMUM_HEARTBEAT_VALUE) ret.add(i);
                //his hearthbeat is too big
                else {
                    //invaliding position
                    activeNodes[i] = EMPTY;
                    offlineNodes[i] = PRESENT;

                }

        }

        //return the arrayList of valid nodes ids
        return ret;

    }





    //Getters and Setters
    public int[] getActiveNodes() {
        return activeNodes;
    }

    public void setActiveNodes(int[] activeNodes) {
        this.activeNodes = activeNodes;
    }

    public int[] getOfflineNodes() {
        return offlineNodes;
    }

    public void setOfflineNodes(int[] offlineNodes) {
        this.offlineNodes = offlineNodes;
    }

    public int getnNodes() {
        return nNodes;
    }

    public void setnNodes(int nNodes) {
        this.nNodes = nNodes;
    }











}

