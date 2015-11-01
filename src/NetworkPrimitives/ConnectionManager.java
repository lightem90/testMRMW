package NetworkPrimitives;

import EncoderDecoder.EncDec;
import Structures.Counter;
import Structures.Message;
import Structures.Tag;
import Structures.View;
import com.robustMRMW.Node;
import electMasterService.electMasterService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by Matteo on 22/07/2015.
 */
public class ConnectionManager {

    public final static String SEPARATOR = "/";
    public final static String ACK_SEPARATOR = "_";

    //Class constants
    private final static String ADDRESS_PATH = "address.txt";
    private final static int PORT = 3000;


    // Class buffer
    private static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    // Class connection variables
    private SocketAddress hostAddress;
    private Selector selector;
    private ArrayList<SocketChannel> serverChannels;
    private ArrayList<InetSocketAddress> nodesToConnect;
    private Map<Integer,InetSocketAddress> otherNodesAddress;

    //Here I store the pair Tag - View that I use to answer
    private Map<Tag, View> rep;
    //Here I store the View sent by the sender (should be the one dictated by the leader)
    private Map<Integer,Message> replica;
    //Here I store the proposedView of the Leader Election procedure, should be the FD of the nodes as well
    private Map<Integer,Message> replicaPropView;

    // Custom objects
    private Communicate comm;
    private Node n;
    private EncDec ED;

    //numeric identifier for each phase
    private Node.State state;


    // Initialization
    public ConnectionManager(Node c){


        rep = new HashMap<>();
        ED = new EncDec();
        n = c;

        //initializing rep with first tag and current view (nobody active), the view will change as soon as we receive messages from other nodes
        rep.put(n.getLocalTag(),n.getLocalView());


    }

    /* initialize and bind selector to current address / port */
    public ArrayList<Integer> init() {

        nodesToConnect = new ArrayList<>();
        otherNodesAddress = new HashMap<>();
        serverChannels = new ArrayList<>();
        replica = new HashMap<>();
        Selector socketSelector = null;


        ArrayList<Integer> ids = new ArrayList<>();

        try {
            int i;
            // read addresses from file
            Path filePath = Paths.get(ADDRESS_PATH);
            List<String> lines = Files.readAllLines(filePath);

            // for each line if it's not my port I'll add the address to the array of addresses
            for (i = 0; i < lines.size(); i++) {

                //EMULAB strings are being removed since we won't use it anymore
                String line = lines.get(i);
                System.out.println(line);
                //position 0 address, 1 port/id
                String[] tokens = line.split(":");

                //filling an array with all the ids to pass it to FD and a map with id-address that may be useful later on
                ids.add(Integer.parseInt(tokens[1]));
                otherNodesAddress.put(Integer.parseInt(tokens[1]), new InetSocketAddress(tokens[0],Integer.parseInt(tokens[1])));
                //otherNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1]))); //local

                //String[] portTokens = tokens[1].split(":"); //local
                //ignoring smaller ids
                if (Integer.parseInt(tokens[1]) < n.getSettings().getNodeId())
                    continue;

                if (Integer.parseInt(tokens[1]) == n.getSettings().getNodeId())
                    hostAddress = new InetSocketAddress(tokens[0],n.getSettings().getNodeId());
                   //hostAddress = new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
                else {
                    //id is bigger than my id
                    nodesToConnect.add(new InetSocketAddress(tokens[0],Integer.parseInt(tokens[1])));
                   //nodesToConnect.add(new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
                }
            }

            try {

                socketSelector = Selector.open();
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                serverChannel.configureBlocking(false);
                System.out.println("Listening socket at: "+hostAddress.toString());
                serverChannel.socket().bind(hostAddress);
                hostAddress = serverChannel.getLocalAddress();
                serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
                selector = socketSelector;
                comm = new Communicate(n);

            } catch (IOException e) {

                System.out.println("Cannot initialize selector");
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //returns an array with the ids of all nodes in the network (read from address.txt file). The quorum is ids.size()/2
        return ids;
    }

    /* Connecting to id-bigger nodes */
    public void connect() throws IOException {

        SelectionKey key = null;
        for (SocketAddress toAdd : nodesToConnect)
        {
            SocketChannel channelToAdd = SocketChannel.open();
            channelToAdd.configureBlocking(false);
            channelToAdd.connect(toAdd);
            key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);

        }
    }


    /* Servicing method, starts one thread for servicing requests and one for user input  */
    public void run() {

            try {

                // listening for connections, initializes the selector with all socket ready for I/O operations
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys()
                        .iterator();
                while (selectedKeys.hasNext()) {

                    // for each element in the selector iterator
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        // if a channel is closed we skip it
                        continue;
                    }


                    if (key.isAcceptable()) {
                        accept(key);
                    } else if (key.isReadable()) {

                        String[] tokens = readMessages(key);
                        System.out.println("Received " + tokens.length + " message/s");

                        //For each message read from buffer
                        for (String msg : tokens) {

                            Message m = ED.decode(msg);
                            System.out.println("Received " + m.getRequestType() + " from node #" + m.getSenderId());
                            //ignoring invalid messages (sender -1 is invalid)
                            if (m.getSenderId() == -1)
                                //Discard invalid message
                                continue;

                            //Should be a valid message: updating FD
                            n.getFD().updateFDForNode(m.getSenderId());
                            if (state == Node.State.ANSWERING) {
                                //Most probable case is handled immediately, no ongoing operation so I just answer
                                //Messages with "_ack" are discarded automatically
                                answer(m, (SocketChannel) key.channel());
                                continue;
                            }

                            //If I'm here I'm performing some operations through communicate class BUT I have still to answer
                            String[] reqToken = m.getRequestType().split(ACK_SEPARATOR);

                            if (reqToken.length == 2) {
                                //Meaning is a response message (_ack) to handle properly by the communicate class
                                Node.State nextState = null;

                                switch (state) {

                                    case WRITING:
                                        //If I'm querying I still have to check if this is the answer of a query request and not of an old phase
                                        nextState = comm.handleWriteMsg(m, reqToken[0]);
                                        if (nextState != state) {
                                            System.out.println("I'm done " + state + ", now I'm " + nextState);
                                            state = nextState;
                                        }
                                        break;

                                    case READING:
                                        //If I'm querying I still have to check if this is the answer of a query request and not of an old phase
                                        state = comm.handleReadMsg(m,reqToken[0]);
                                        if (nextState != state) {
                                            System.out.println("I'm done " + state + ", now I'm " + nextState);
                                            state = nextState;
                                        }
                                        break;

                                    case ANSWERING:
                                        //Receive an ack of an old request, do nothing
                                        break;

                                    default:
                                        //Should be unreachable
                                }

                            }
                            else
                                //Not an ack so I just have to answer
                                answer(m,(SocketChannel)key.channel());

                            /* TODO: check the right position for this
                            //This must start as soon as we know a quorum of nodes is detected or the leader goes offline
                            if (n.getFD().getActiveNodes().size() >= n.getSettings().getQuorum() && n.getFD().getLeader_id() == -1)
                               startElectionRoutine();
                            */
                        }


                    } else if (key.isConnectable())
                        finishConnection(key);

                    }


            } catch (Exception e) {
                e.printStackTrace();
            }


    }

    /* Answering to the received request in different ways depending upon the request. The important thing to notice is that it replies with the same request type (idSender + rndID) and _ack */
    private void answer(Message receivedMessage, SocketChannel channel) {

        Tag maxTag = findMaxTagFromSet(rep);
        //Splitting: the first field is a redundant for who is sending the request and the second a rnd number identifying the phase
        String tokenType[] = receivedMessage.getRequestType().split(SEPARATOR);


            switch (tokenType[2]) {

                case "query":
                    sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", maxTag, rep.get(maxTag), n.getSettings().getNodeId()));
                    //saving proper message in replica map, in this way I can retrieve the node view
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    break;

                case "pre-write":

                    //In the case I receive a pre-write message, if the tag is the highest I know i set the new view as .PRE, otherwise I send an invalid ack
                    Tag newTag = receivedMessage.getTag();
                    if (maxTag.compareTo(newTag) >= 0) {
                        System.out.println("Received tag smaller than local max tag");
                        sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", new Tag(-1,-1,-1), new View (""), n.getSettings().getNodeId()));
                        break;
                    }
                    else {

                        //I think this is wrong, if a prewrite is received it shouldn't become localView now. It should wait the finalize message and until that should be stored in rep
                        //n.setLocalTag(newTag);
                        //n.setLocalView(receivedMessage.getView());
                        n.getLocalView().setStatus(View.Status.PRE);
                        rep.put(receivedMessage.getTag(), receivedMessage.getView());
                        //Responding with receivedTag. Using the attribute from message instead of newTag for readability
                        sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", receivedMessage.getTag(), receivedMessage.getView(), n.getSettings().getNodeId()));
                        //saving proper message in replica map, in this way I can retrieve the node view
                        replica.put(receivedMessage.getSenderId(), receivedMessage);
                    }
                    break;

                case "finalize":
                    Tag bestTag = receivedMessage.getTag();

                    if (rep.containsKey(bestTag)) {
                        rep.get(bestTag).setStatus(View.Status.FIN);
                        //View is finalized I can use that as local now
                        n.setLocalTag(bestTag);
                        n.setLocalView(rep.get(bestTag));
                        sendMessage(channel, new Message(receivedMessage.getRequestType() + ACK_SEPARATOR + "ack", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId()));
                        //saving proper message in replica map, in this way I can retrieve the node view
                        replica.put(receivedMessage.getSenderId(), receivedMessage);
                    } else {

                        View tmp = new View("");
                        tmp.setStatus(View.Status.FIN);
                        rep.put(bestTag, tmp);
                        sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", bestTag, tmp, n.getSettings().getNodeId()));
                    }

                    break;

                case "gossip":

                    Tag latestTag = receivedMessage.getTag();
                    if (rep.containsKey(latestTag))
                        //storing a finalized view from a gossip message. TODO: is this right? (check the algorithm)
                        rep.put(latestTag, receivedMessage.getView());

                    //saving proper message in replica map, in this way I can retrieve the node view
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    break;
                //Reading -1
                default:
                    System.out.println("Server or reader/writer crashed " +receivedMessage.getRequestType());

            }


    }





    //Networking functions**********************************************************************************************

    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
        //add the accepted socket to the list of active servers
        serverChannels.add(socketChannel);
        //and updating channels of communicate
        comm.setChan(serverChannels);

        System.out.println("Client connected");
    }

    /* method for establishing connection to other server */
    private void finishConnection(SelectionKey key) throws IOException {

        SocketChannel socketChannel = (SocketChannel) key.channel();
        if (!serverChannels.contains(socketChannel)) {
            try {
                // If the channel is not in our list of connected sockets, finish the connection.
                // If the connection operation failed this will raise an IOException.
                if (socketChannel.finishConnect())
                    key.cancel();

                if (socketChannel.isConnected()) {
                    serverChannels.add(socketChannel);

                    //update view as soon as I see an active connection
                    for (Integer id : otherNodesAddress.keySet()) {

                        //Yes no maybe? xD
                        if (otherNodesAddress.get(id).equals(socketChannel.getRemoteAddress())) {
                            System.out.println("Detected connection from:" + id + ", updating FD");
                            n.getFD().updateFDForNode(id);
                        }
                    }
                }

                //TODO: What happens to old channels when I do setChan to older chans in communicate? For notw setChan updates the communicate chans checking the differences
                comm.setChan(serverChannels);

            } catch (IOException e) {

                removeChannelFromList(socketChannel);
                removeChannelFromComm(socketChannel);
                reRegisterKey(socketChannel);
                return;
            }
        }
    }

    /* Close all the channels of from this node to the sockets */
    public boolean stop() {

        System.out.println("Stopping the node");
        if (selector != null) {
            try {
                selector.close();
                for (SocketChannel s : serverChannels) {
                    s.socket().close();
                    s.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // deleteAddressFromFile();
        return true;

    }



    //Algorithm**************************************************************************************************************************

    private void startElectionRoutine(){

        System.out.println("This replica (CM) contains: " + replica.keySet().toString());
        electMasterService election = new electMasterService(n.getSettings(), n.getLocalView(), this, n.getFD().getActiveNodes());
        int leader = election.electMaster();
        n.getFD().setLeader_id(leader);
        System.out.println("Master elected with id: "+ n.getFD().getLeader_id());
            if (leader == -1){
                System.out.println("No suitable leader, querying...");
                write(n.getProposedView());
                //read();
                //System.out.println("Query ended");
                return;

            }
            if (leader == n.getSettings().getNodeId()) {
                n.setIsMaster(true);
                n.setLocalView(n.getProposedView());
                write(n.getProposedView());
            }

    }
    //Writes a view
    public void write(View view){

        //nothing to do here..
        System.out.println("Writing: " + view.getValue());
        comm.write(view);

    }

    public void write(){

        //nothing to do here..
        System.out.println("Writing: " + n.getLocalView().getValue());
        comm.write(n.getLocalView());

    }

    public void read(){

        System.out.println("Reading");
        comm.read();

    }




    //Utilities*******************************************************************************************************************

    /* finds the highest tag in rep map */
    private Tag findMaxTagFromSet(Map<Tag, View> map) {

        //the minimum tag that we have is the localTag (if it is a valid view, otherwise it is the smallest possible tag)
        Tag maxTag;
        if (n.getLocalView().getStatus() == View.Status.FIN)
            maxTag = n.getLocalTag();
        else maxTag = new Tag(0,0,0);

        Set<Tag> tags = map.keySet();
        for (Tag tag : tags) {
            if (map.get(tag).getStatus() == View.Status.PRE)
                continue;
            if (tag.compareTo(maxTag) > 0) {
                maxTag = tag;
            }

        }

        return maxTag;

    }

    /* This function takes old Tag and decide if adding an element to counter list or updating label (just +1 in our case) */
    private Tag generateNewTag(Tag lastTag) {

        int id = n.getSettings().getNodeId();
        //if counter is exhausted I start with a new label and set new counter to 0
        if (lastTag.isExhausted())
            return new Tag(id,lastTag.getLabel()+1,0);

        //counter not exhausted so just adding the new value, first 2 lines should be useless
        lastTag.setId(id);
        lastTag.setLabel(lastTag.getLabel());
        int newCVal = (int) ((lastTag.getCounters().getFirst().getCounter())+1);
        lastTag.addCounter(new Counter(id,newCVal));



        return lastTag;
    }


    /* writes message into buffer */
    private void sendMessage(SocketChannel s, Message m) {

        String remoteAdd=null;
        try {
        remoteAdd =  s.getRemoteAddress().toString();
        System.out.println("Trying to send: " + m.getRequestType() + " to " +remoteAdd);

        if (s.isConnected()){

            System.out.println("Sending: " + m.getRequestType() + " to " + remoteAdd);
            writeBuffer.clear();
            writeBuffer.put(ED.encode(m).getBytes());
            writeBuffer.flip();

            while (writeBuffer.hasRemaining())
                    s.write(writeBuffer); // writing messsage to server

            writeBuffer.clear();
            }

        }  catch (IOException e) {
            System.out.println("Error in writing message to: "+remoteAdd);
            reRegisterKey(s);
            serverChannels.remove(s);
            comm.setChan(serverChannels);
        }


    }

    private String[] readMessages(SelectionKey key) {

        String message = "";
        try{

            readBuffer.clear();

            SocketChannel channel = (SocketChannel) key.channel();
            while (channel.read(readBuffer) > 0) {
                // flip the buffer to start reading
                readBuffer.flip();
                message += Charset.defaultCharset().decode(readBuffer);
            }

        } catch (IOException e) {
            System.out.println("Server or reader/writer crashed in read");
            reRegisterKey((SocketChannel)key.channel());
            key.cancel();
            serverChannels.remove(key.channel());
            comm.setChan(serverChannels);
        }

        return message.split("&");

    }

    //Da usare? non credo
    void removeChannelFromList(SocketChannel toRemove){

        if (serverChannels.contains(toRemove))
            serverChannels.remove(toRemove);

    }

    void removeChannelFromComm(SocketChannel toRemove) {

        if (comm.getChan().contains(toRemove))
            comm.getChan().remove(toRemove);
    }

    /* At the moment we re-register a channel only if it crashes while reading */
    void reRegisterKey(SocketChannel s){
        try {
        SelectionKey key = null;
        if (s.isOpen())
            key = s.register(selector, SelectionKey.OP_CONNECT);
        } catch (ClosedChannelException e) {
            System.out.println("Cannot re-register key/channel");
            e.printStackTrace();
        }


    }



    /* Getters and Setters */
    public ArrayList<SocketChannel> getServerChannels() {
        return serverChannels;
    }

    public void setServerChannels(ArrayList<SocketChannel> serverChannels) {
        this.serverChannels = serverChannels;
    }


    public Map<Integer, Message> getReplica() {
        return replica;
    }

    public void setReplica(Map<Integer, Message> replica) {
        this.replica = replica;
    }

    public Node.State getState() {
        return state;
    }

    public void setState(Node.State state) {
        this.state = state;
    }


}
