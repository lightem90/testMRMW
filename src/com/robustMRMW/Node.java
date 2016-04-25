package com.robustMRMW;

import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.Epoch;
import Structures.Tag;
import Structures.View;
import java.util.*;

public class Node {

    public boolean flag;

    public enum State{

        ANSWERING, WRITING, READING

    }

    public enum Status {
        NONE(0),
        MULTICAST(1),
        PROPOSE(2),
        INSTALL(3);
        private final int value;

        private Status(int value)
        {this.value = value;}

        public int GetValue()
        {return this.value;}
    }


    // Custom Class variables and object
    private View localView;
    private View proposedView;
    private Tag localTag;
    private Settings settings;
    private FailureDetector FD;
    private ConnectionManager cm;
    private Status status;

    // flag to indicate if this node is the master
    private boolean isMaster;



    /* Constructor with custom settings */
    public Node(Settings settings) {

        this.settings = settings;
        //initializing initial tag with all zeroes and my id, localView at start it is just me active
        localTag = new Tag(new Epoch(this.getSettings().getNodeId(),0),0);
        localView = new View(String.valueOf(this.settings.getNodeId()));
        localView.setArrayFromValueString();
        cm = new ConnectionManager(this);
        setIsMaster(false);


    }


    public void setup(){

        //retrieving the array of server ids
        ArrayList<Integer> ids = cm.init();
        if (ids.isEmpty()){

            System.out.println("Error-> No nodes detected");
            return;
        }
        settings.setNumberOfNodes(ids.size());
        settings.setQuorum((ids.size()/2)+1);
        status = Status.NONE;
        FD = new FailureDetector(ids,this);

        System.out.println("Setting up node in system with: " + settings.getNumberOfNodes() + " nodes, " + settings.getQuorum() + " quorum");
        cm.connect();
    }


    public void run(){

        flag = true;
        System.out.println("Running ...");
        while (true) {

            //if (FD.getActiveNodes().size() >= settings.getQuorum() && FD.getLeader_id() == -1)
            //    cm.startElectionRoutine();
            cm.run();

            cm.checkForLeaderElection();

        }

    }



    /* Getters and Setters */

    public FailureDetector getFD() {
        return FD;
    }

    public void setFD(FailureDetector FD) {
        this.FD = FD;
    }


    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
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
        return (localTag==null ? new Tag(new Epoch(-1,-1),-1) : localTag);
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
