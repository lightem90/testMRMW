package NetworkPrimitives;

/**
 * Created by Matteo on 22/07/2015.
 */
public class Settings {

    //Class used to attach command line parameters to a node so they can easily retrieved (address, port, all kind of data we will need)


    String address;
    int port;
    int nodeId;

    public Settings(String add, int p, int n){

        address = add;
        port = p;
        nodeId = n;
    }




    /* Getters and Setters */
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }


    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }



}
