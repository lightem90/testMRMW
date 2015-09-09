package com.robustMRMW;

import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.Tag;
import Structures.View;
import electMasterService.electMasterService;

import java.io.IOException;
import java.util.*;

public class Node {


    // Class variables and object
    private View localView;
    private View proposedView;
    private Tag localTag;
    private Settings mySett;
    private FailureDetector FD;
    private ConnectionManager cm;


    private boolean isMaster;
    private int quorum;
    private int numberOfNodes;


    /* Constructor with custom settings */
    public Node(Settings settings) {

        mySett = settings;
        //initializing initial tag with all zeroes and my id, localView at start it is just me active
        localTag = new Tag(mySett.getNodeId(),0,0);
        localView = new View(String.valueOf(mySett.getNodeId()));
        localView.setArrayFromValueString();
        cm = new ConnectionManager(this);
        setIsMaster(false);


    }


    public void setup(){

        //retrieving the array of server ids
        ArrayList<Integer> ids = cm.init();
        numberOfNodes = ids.size();
        quorum = numberOfNodes /2;
        FD = new FailureDetector(ids,this);
        try {

            //TODO: cm should retrieve leader_id if it is present somewhere

            cm.connect();
        } catch (IOException e){


        }



    }


    public void run(){


        if(FD.getActiveNodes().size() < quorum)

            cm.waitForQuorum();


        else {

            if (FD.getLeader_id() == -1) {
                electMasterService election = new electMasterService(localView, cm, FD.getActiveNodes());
            }

            else
                //usual run
                cm.run();
        }





    }





    /* Getters and Setters */



    public FailureDetector getFD() {
        return FD;
    }

    public void setFD(FailureDetector FD) {
        this.FD = FD;
    }


    public Settings getMySett() {
        return mySett;
    }

    public void setMySett(Settings mySett) {
        this.mySett = mySett;
    }


    public View getLocalView() {
        return (localView==null ? new View("") : localView);
    }

    public void setLocalView(View localView) {
        this.localView = localView;
    }

    public Tag getLocalTag() {
        return (localTag==null ? new Tag(-1,-1,-1) : localTag);
    }

    public void setLocalTag(Tag localTag) {
        this.localTag = localTag;
    }

    public View getProposedView() {
        return proposedView;
    }

    public ArrayList<Integer> getProposedViewArray() {
        return proposedView.getIdArray();
    }

    public void setProposedView(View proposedView) {
        this.proposedView = proposedView;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setIsMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }


    public int getQuorum() {
        return quorum;
    }

    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }


    public ConnectionManager getCm() {
        return cm;
    }

    public void setCm(ConnectionManager cm) {
        this.cm = cm;
    }

}
