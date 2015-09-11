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
        mySett.setNumberOfNodes(ids.size());
        mySett.setQuorum((ids.size()/2)+1);
        FD = new FailureDetector(ids,this);

        System.out.println("Setting up node in system with: " + mySett.getNumberOfNodes() + " nodes, " + mySett.getQuorum() + " quorum");

        try {
            //TODO: cm should retrieve leader_id if it is present somewhere
            cm.connect();
        } catch (IOException e){


        }



    }


    public void run(){


        while(true) {
            if (FD.getActiveNodes().size() < mySett.getQuorum())

                cm.waitForQuorum();


            else {

                if (FD.getLeader_id() == -1) {
                    System.out.println("Master is not present in the system, starting leader election routine");
                    electMasterService election = new electMasterService(mySett, localView, cm, FD.getActiveNodes());
                    election.electMaster();
                    System.out.println("Master elected with id: "+ FD.getLeader_id());
                }

                if (FD.getActiveNodes().size() >= mySett.getQuorum()) {
                    System.out.println("Run allowed with leader id: "+ FD.getLeader_id() +", quorum of " + FD.getActiveNodes().size()+" nodes and localView:" + localView.getValue());
                    cm.run();
                }
                break;
            }
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




    public ConnectionManager getCm() {
        return cm;
    }

    public void setCm(ConnectionManager cm) {
        this.cm = cm;
    }

}
