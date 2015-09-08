package electMasterService;

/**
 * Created by Matteo on 13/07/2015.
 */

import NetworkPrimitives.ConnectionManager;
import Structures.Message;
import com.robustMRMW.Node;
import Structures.View;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {

    private int quorum;
    private int numberOfNodes;
    private Map<Integer,Integer> failureDetector;
    private ArrayList<Message> rep;
    private Map<Integer, Message> repPropView;
    private ArrayList<Integer> seemCrd;
    private ArrayList<SocketChannel> chan;
    private int serverCount;
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);


    private boolean noCrd = false;
    private boolean imCrd = false;


    /*TODO: How do we start the leader election routine and how we get this structures? More messages? */
    /*TOREPLY: leader election starts when the failure detector says the current leader is dead*/

    public electMasterService(ConnectionManager cm, int networkNodes, Map<Integer,Integer> nodeFailureDetector, ArrayList<Message> repliesArray, Map<Integer, Message> repliesArrayPropView ){

        numberOfNodes = networkNodes;
        failureDetector = nodeFailureDetector;
        quorum = numberOfNodes/2;

        rep = repliesArray;
        repPropView = repliesArrayPropView;

        chan = cm.getServerChannels();
        serverCount = cm.getServerCount();
    }

    public void findPossibleMasters(){

        seemCrd = new ArrayList<>(numberOfNodes);

        for (Message m : rep){

            View nodeView = m.getView();
            nodeView.setArrayFromValueString(); //now I have all the ids in the view array

            Message curr = repPropView.get(m.getSenderId());

            View propView = curr.getView();
            propView.setArrayFromValueString();

            //isContained checks if the sender of the proposed view is contained in each View of the nodes in his proposedView
            if ((nodeView.getIdArray().size() > quorum) &&  propView.getIdArray().size() > quorum && isContained(curr.getSenderId(), propView))
                seemCrd.add(m.getSenderId());
        }

        if (seemCrd == null || seemCrd.isEmpty()) noCrd = true;
        proposeMaster();

    }

    public void proposeMaster(){

        int masterId = -1;

        if(noCrd){
           /*
           * for every node in my FD that has ME in their FD, count those that have the noCrd set to true
           * if these nodes with noCrd reach a quorum, I propose my FD as View (acting like a leader ??)
           * */


            //Send "noCrd" to everyone letting them know that we have no coordinator
            writeBuffer.clear();
            writeBuffer.put("noCrd".getBytes());

            for (int i = 0; i < serverCount; i++) {

                try {

                    writeBuffer.flip();
                    while (writeBuffer.hasRemaining())
                        chan.get(i).write(writeBuffer);

                } catch (IOException e) {
                    //if write fails it means that the channel has been closed so we cannot write to it
                    //should remove channel (???)
                }
            }

            //Start receiving other nodes' "noCrd" and count them until quorum is reached
            int counter=0;
            while (counter < serverCount / 2 + 1) {
                for (int i = 0; i < serverCount; i++) {
                    readBuffer.clear();
                    String message = "";
                    try {

                        while (chan.get(i).read(readBuffer) > 0) {
                            // flip the buffer to start reading
                            readBuffer.flip();
                            message += Charset.defaultCharset().decode(
                                    readBuffer);
                        }

                        if (message.equals("noCrd"))
                            counter++;

                    } catch (IOException e) {
                        //if write fails it means that the channel has been closed so we cannot write to it
                        //should remove channel (???)
                    }
                }
            }
        }
        else {
            seemCrd.sort(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            masterId = seemCrd.get(0);
        }

        //TODO: up to now, the noCrd part of the code does not change masterId, but simply leaves it to -1 and arrives here only if quorum is reached. This should not be the final solution
        electMaster(masterId);

    }

    public int electMaster(int mId){

        if(mId == -1){
            //noCrd must be true and quorum was reached between servers
            //should send my view to everyone and let the dicks sort themselves out
        }
        else{
            //real master was elected, WOW
            if(imCrd)
                //I'm the leader, decide what to do
            ;
            else
                //do nothing
            ;
        }

    return 0;
    }



    private boolean isContained (int l, View pView){


        //for each nodeID in the propView
        for(int i : pView.getIdArray()){

            Message m = rep.get(i);
            View v = m.getView();
            v.setArrayFromValueString();

            //Check if l is in the view (list of active nodes of node i)
            if (v.getIdArray().contains(l))
                return true;



        }


        return false;
    }







}
