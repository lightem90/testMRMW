package com.robustMRMW;

import NetworkPrimitives.Communicate;
import NetworkPrimitives.ConnectionManager;
import NetworkPrimitives.Settings;
import Structures.Message;
import Structures.Tag;
import Structures.View;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Node {


    // Class variables and object
    private View localView;
    private View proposedView;
    private Tag localTag;
    private Settings mySett;


    private ConnectionManager cm;
    private boolean isMaster;


    /* Constructor with custom settings */
    public Node(Settings settings) {

        mySett = settings;
        cm = new ConnectionManager(this);
        setIsMaster(false);


    }


    public void setup(){

        cm.init();
        try {
            cm.connect();
        } catch (IOException e){
            //TODO: handle

        }



    }

    public void run(){

        cm.run();


    }





    /* Getters and Setters */


    public Settings getMySett() {
        return mySett;
    }

    public void setMySett(Settings mySett) {
        this.mySett = mySett;
    }


    public View getLocalView() {
        return localView;
    }

    public void setLocalView(View localView) {
        this.localView = localView;
    }

    public Tag getLocalTag() {
        return localTag;
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
