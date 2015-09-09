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
import java.util.Set;

//as stated in the paper, probably we have to implement other messages to implement the election procedure (acknowledge of missing master for example)
//moreover we should keep for each server the last message from it into a list, in this way we can retrieve its id and view, OR asking from it again with more messages
public class electMasterService {


    private Map<Integer,Integer> failureDetector;
    private Map<Integer, Message> rep;
    private ArrayList<Integer> seemCrd;
    private ArrayList<SocketChannel> chan;

    private int serverCount;
    private int mId;
    private View view;
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);


    private boolean noCrd = false;
    private boolean imCrd = false;


    /*TODO: Where do we detect current leader is dead? */

    public electMasterService(View localView, ConnectionManager cm, Map<Integer,Integer> nodeFailureDetector){

        failureDetector = nodeFailureDetector;
        view = localView;
        rep = cm.getReplica();

        chan = cm.getServerChannels();
        serverCount = cm.getServerCount();

    }

    public void findPossibleMasters(){

        seemCrd = new ArrayList<>(serverCount);
        int quorum = serverCount/2;
        Set<Integer> idList = failureDetector.keySet();

        for (Integer l : idList){

            //Getting all the information about active node l (Last message with proper view received)
            Message m = rep.get(l);
            View nodeView = m.getView();
            nodeView.setArrayFromValueString();

            //isContained checks if l has a quorum for his view and if he is contained in each view of the nodes of his view (can't check proposed view and FD since we don't have replicas)
            if ((nodeView.getIdArray().size() > quorum) && isContained(nodeView, l))
                seemCrd.add(l);
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

    private int electMaster(int mId){

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



    private boolean isContained (View mView, int id){


        //for each nodeID in the propView
        for(int i : mView.getIdArray()){

            Message m = rep.get(i);
            View v = m.getView();
            v.setArrayFromValueString();

            //Check if mId is in the view (list of active nodes of node i)
            if (v.getIdArray().contains(id))
                return true;
        }


        return false;
    }







}
