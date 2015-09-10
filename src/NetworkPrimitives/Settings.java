package NetworkPrimitives;

import Structures.View;

/**
 * Created by Matteo on 22/07/2015.
 */
public class Settings {

    //Class used to attach command line parameters to a node so they can easily retrieved (address, port, all kind of data we will need)
    //We may want to add other variables used both by Connection Manager and Communicate to make work easier


    private int port;
    private int nodeId;
    private int quorum;
    private int numberOfNodes;

    public Settings(int n, int p){

        nodeId = n;
        port = p;


    }




    /* Getters and Setters */
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
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



}
