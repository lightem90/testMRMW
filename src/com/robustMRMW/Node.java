package com.robustMRMW;

import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.Tag;
import Structures.View;

import java.io.IOException;
import java.util.*;

public class Node {

    public enum State{

        ANSWERING, WRITING, READING

    }


    // Class variables and object
    private View localView;
    private View proposedView;
    private Tag localTag;
    private Settings settings;
    private FailureDetector FD;
    private ConnectionManager cm;

    private final  static  int RANDOM_SEED = 60;


    private boolean isMaster;


    /* Constructor with custom settings */
    public Node(Settings settings) {

        this.settings = settings;
        //initializing initial tag with all zeroes and my id, localView at start it is just me active
        localTag = new Tag(this.settings.getNodeId(),0,0);
        localView = new View(String.valueOf(this.settings.getNodeId()));
        localView.setArrayFromValueString();
        cm = new ConnectionManager(this);
        setIsMaster(false);


    }


    public void setup(){

        //retrieving the array of server ids
        ArrayList<Integer> ids = cm.init();
        settings.setNumberOfNodes(ids.size());
        settings.setQuorum((ids.size()/2)+1);
        FD = new FailureDetector(ids,this);

        System.out.println("Setting up node in system with: " + settings.getNumberOfNodes() + " nodes, " + settings.getQuorum() + " quorum");
        cm.connect();
    }


    public void run(){


        System.out.println("Running ...");
        while (true) {

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


    public Settings getSettings() {
        return settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
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
