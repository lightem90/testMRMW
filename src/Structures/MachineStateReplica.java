package Structures;

import com.robustMRMW.Node;

/**
 * Created by Matteo on 05/01/2016.
 */
public class MachineStateReplica {


    private int id;                             //Id of the replicated machine
    private int leaderId;
    private int rnd;                            //Last round number
    private View view;                          //Last multicast view valid for a quorum of nodes
    private View propView;                      //proposedView during propose phase, the same as FD!
    private Message lastMessage;                //Last received message
    private Message input;                      //Last received multicast message
    private Node.Status status;                 //Status of the machine


/* Getters and Setters */

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getRnd() {
        return rnd;
    }

    public void setRnd(int rnd) {
        this.rnd = rnd;
    }

    public View getView() {
        return view;
    }

    public void setView(View view) {
        this.view = view;
    }

    public View getPropView() {
        return propView;
    }

    public void setPropView(View propView) {
        this.propView = propView;
    }

    public Message getLastMessage() {
        return lastMessage;
    }

    public void setLastMessage(Message lastMessage) {
        this.lastMessage = lastMessage;
    }

    public Message getInput() {
        return input;
    }

    public void setInput(Message input) {
        this.input = input;
    }

    public Node.Status getStatus() {
        return status;
    }

    public void setStatus(Node.Status status) {
        this.status = status;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }


}
