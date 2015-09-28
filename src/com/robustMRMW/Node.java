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

    private final  static  int RANDOM_SEED = 60;


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

            cm.connect();

        } catch (IOException e){

            System.out.println("Error while connecting node");

        }



    }


    public void run(){


        System.out.println("Running ...");
        while (true) {

            //answering requests
            cm.run();/*
            if (FD.getLeader_id() != -1 && FD.getActiveNodes().size() >= mySett.getQuorum()) {

                Thread operate = new Thread() {
                public void run() {
                    try {
                        Thread.sleep(RANDOM_SEED * 1000);

                        if (isMaster)
                            cm.operationMaster();
                        else cm.operation();

                    } catch(InterruptedException v) {
                        System.out.println(v);
                    }
                }
            };

            operate.start();
            }
            /*
            if (FD.getLeader_id() != -1 && FD.getActiveNodes().size() >= mySett.getQuorum()) {
                Random r = new Random();
                try {
                    Thread.sleep(RANDOM_SEED * 1000);
                } catch (InterruptedException e) {
                    System.out.println("Sleep failed");
                    e.printStackTrace();
                }
                cm.operation();
            }
            */


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
