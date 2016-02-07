package NetworkPrimitives;

import EncoderDecoder.EncDec;
import Structures.*;
import com.robustMRMW.Node;
import electMasterService.electMasterService;

import java.io.IOException;
import java.net.ConnectException;
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

    /* Class constants */
    private final static String ADDRESS_PATH = "address.txt";
    private final static int PORT = 3000;
    private final static int INVALID = -1;


    /* Class buffer */
    private static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    /* Class connection variables */
    private SocketAddress hostAddress;
    private Selector selector;
    private ArrayList<SocketChannel> connectedServerChannels;
    private ArrayList<InetSocketAddress> nodesToConnect;
    private Map<Integer,InetSocketAddress> allNodesAddress;

    //Here I store the pair Tag - View that I use to answer
    private Map<Tag, View> tagViewMap;
    //Here I store the View sent by the sender (should be the one dictated by the leader)
    private Map<Integer,Message> lastDeliveredMessageMap;
    //Map of replicated state machines, one for each node in the system
    private Map<Integer,MachineStateReplica> rep;

    // Custom objects
    private Communicate comm;
    private Node n;
    private EncDec ED;

    private Node.State state;
    private int rnd;

    // Initialization
    public ConnectionManager(Node c){


        tagViewMap = new HashMap<>();
        rep = new HashMap<>(c.getSettings().getNumberOfNodes());
        ED = new EncDec();
        n = c;

        //initializing tagViewMap with first tag and current view (nobody active), the view will change as soon as we receive messages from other nodes
        tagViewMap.put(n.getLocalTag(), n.getLocalView());

        rnd = 0;


    }

    /* initialize and bind selector to current address / port */
    public ArrayList<Integer> init() {

        nodesToConnect = new ArrayList<>();
        connectedServerChannels = new ArrayList<>();
        allNodesAddress = new HashMap<>();
        lastDeliveredMessageMap = new HashMap<>();
        Selector socketSelector = null;


        ArrayList<Integer> ids = new ArrayList<>();

        try {
            // read addresses from file
            Path filePath = Paths.get(ADDRESS_PATH);
            List<String> lines = Files.readAllLines(filePath);

            if (!readAddressesFromFile(lines,ids)) {
                System.out.println("Error in reading addresses");
                return new ArrayList<>();
            }

            //initializes the conection
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
            System.out.println("Error in opening address file");
            e.printStackTrace();
        }

        //initializes all the replicas
        for (int i : ids)
        {
            MachineStateReplica tmp = new MachineStateReplica();
            tmp.setId(i);
            rep.put(i,tmp);
        }

        //returns an array with the ids of all nodes in the network (read from address.txt file). The quorum is ids.size()/2
        return ids;
    }

    /* Connecting to id-bigger nodes */
    public void connect(){

        /* Register all the not yet connected nodes to the selector */
        SelectionKey key = null;
        SocketChannel channelToAdd = null;
        for (SocketAddress toAdd : nodesToConnect)
        {
            try {

                channelToAdd = SocketChannel.open();
                channelToAdd.configureBlocking(false);
                channelToAdd.connect(toAdd);
                System.out.println("Registering " + toAdd.toString() + " to selector for connection");
                key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);

            }catch (IOException e)
            {
                System.out.println("Cannot open channel with address: " + channelToAdd.toString());
                continue;
            }
        }
    }


    /* Servicing method, starts one thread for servicing requests and one for user input  */
    public void run() {

        /* Everytime I register the not yet connected nodes to the selector */

            try {
                // listening for connections, initializes the selector with all socket ready for I/O operations
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys()
                        .iterator();
                while (selectedKeys.hasNext()) {

                    // for each element in the selector iterator
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()){
                        continue;
                    }

                    if (key.isAcceptable()) {
                        accept(key);
                        /* If the key is readable I have to handle the input properly, depending if there are pending operations or I'm just listening and answering to requests */
                    } else if (key.isReadable()) {

                        String[] tokens = readMessages(key);
                        System.out.println("Received " + tokens.length + " message/s");

                        //For each message read from buffer
                        for (String msg : tokens) {

                            Message m = ED.decode(msg);
                            System.out.println("Received " + m.getRequestType() + " from node #" + m.getSenderId());
                            //ignoring invalid messages (sender -1 is invalid)
                            if (m.getSenderId() == INVALID) {
                                //Discard invalid message
                                System.out.println("Discarding invalid sender message");
                                continue;
                            }

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
                                //Meaning is a response message (_ack) to some kind of communicate phase to be handled properly by the communicate class
                                Node.State nextState = null;

                                switch (state) {

                                    case WRITING:
                                        //If I'm querying I still have to check if this is the answer of a query request and not of an old phase
                                        nextState = comm.handleWriteMsg(m, reqToken[0]);
                                        if (nextState != state && nextState != null) {
                                            System.out.println("I'm done " + state + ", now I'm " + nextState);
                                            state = nextState;
                                        }
                                        break;

                                    case READING:
                                        //If I'm querying I still have to check if this is the answer of a query request and not of an old phase
                                        state = comm.handleReadMsg(m,reqToken[0]);
                                        if (nextState != state && nextState != null) {
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


                    }
                    else if (key.isConnectable())
                        finishConnection(key);

                    else if (key.isWritable()){
                        /*
                        System.out.println("print this is the key is writable (OP_WRITE set)");
                        write(new View("testView"));
                        */
                        }
                    }

                if (connectedServerChannels.size() >= n.getSettings().getQuorum() && n.getSettings().getNodeId() == 5001 && n.flag) {
                    write();
                    n.flag = false;
                }
            } catch (Exception e) {
                System.out.println("Server crashed in loop");
                e.printStackTrace();
            }

    }

    /* Answering to the received request in different ways depending upon the request. The important thing to notice is that it replies with the same request type (idSender + rndID) and _ack */
    private void answer(Message receivedMessage, SocketChannel channel) {

        //Shouldn't this be the local tag?
        Tag maxTag = findMaxTagFromSet(tagViewMap);
        //Splitting: the first field is a redundant for who is sending the request and the second a rnd number identifying the phase
        String tokenType[] = receivedMessage.getRequestType().split(SEPARATOR);

            switch (tokenType[2]) {

                case "query":
                    sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", maxTag, tagViewMap.get(maxTag), n.getSettings().getNodeId(),n.getFD().getLeader_id()));
                    //saving proper message in lastDeliveredMessageMap map, in this way I can retrieve the node view
                    lastDeliveredMessageMap.put(receivedMessage.getSenderId(), receivedMessage);
                    /* Syncincg the receveived message, if the message is a query it contains the view and not the proposed view */
                    syncReplica(receivedMessage,false);
                    break;

                case "pre-write":

                    //In the case I receive a pre-write message, if the tag is the highest I know i set the new view as .PRE, otherwise I send an invalid ack
                    Tag newTag = receivedMessage.getTag();

                    System.out.println("Max tag is: " + maxTag.getEpoch().getEpoch() + " and id: "+ maxTag.getCounters().getFirst().getId() +
                            "-" + maxTag.getEpoch().getId() + " and counter: " + maxTag.getCounters().getFirst().getCounter());

                    System.out.println("Max tag is: " + newTag.getEpoch().getEpoch() + " and id: "+ newTag.getCounters().getFirst().getId() +
                            "-" + newTag.getEpoch().getId() + " and counter: " + newTag.getCounters().getFirst().getCounter());
                    if (maxTag.compareTo(newTag) >= 0) {
                        System.out.println("Received tag smaller than local max tag");
                        sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", new Tag(new Epoch(INVALID,INVALID),INVALID), new View (""), n.getSettings().getNodeId(),n.getFD().getLeader_id()));
                        break;
                    }
                    else {

                        //This line sets the new tag as local highest tag.. must wait finalize for this?
                        //n.setLocalTag(newTag);
                        n.getLocalView().setLabel(View.Label.PRE);
                        tagViewMap.put(receivedMessage.getTag(), receivedMessage.getView());
                        //Responding with the last higher tag (that must be this and the written view
                        sendMessage(channel, new Message(receivedMessage.getRequestType()+ACK_SEPARATOR+"ack", receivedMessage.getTag(), receivedMessage.getView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()));
                        //saving proper message in lastDeliveredMessageMap map, in this way I can retrieve the node view
                        lastDeliveredMessageMap.put(receivedMessage.getSenderId(), receivedMessage);
                    }
                    break;

                case "finalize":
                    Tag bestTag = receivedMessage.getTag();
                    /* Syncing the replica with the proposedView */
                    syncReplica(receivedMessage,true);
                    if (tagViewMap.containsKey(bestTag)) {
                        tagViewMap.get(bestTag).setLabel(View.Label.FIN);
                        //View is finalized I can use that as local now
                        n.setLocalTag(bestTag);
                        //This is okay since I do this only after a succeding write operation
                        n.setLocalView(tagViewMap.get(bestTag));
                        System.out.println("Received finalize after correct pre-write/read procedure");
                        sendMessage(channel, new Message(receivedMessage.getRequestType() + ACK_SEPARATOR + "ack", n.getLocalTag(), n.getProposedView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()));
                        //saving proper message in lastDeliveredMessageMap map, in this way I can retrieve the node view
                        lastDeliveredMessageMap.put(receivedMessage.getSenderId(), receivedMessage);
                    } else {

                        View tmp = new View("");
                        tmp.setLabel(View.Label.FIN);
                        tagViewMap.put(bestTag, tmp);
                        System.out.println("Received finalize with an unknown tag");
                        sendMessage(channel, new Message(receivedMessage.getRequestType() + ACK_SEPARATOR + "ack", bestTag, n.getProposedView(), n.getSettings().getNodeId(),n.getFD().getLeader_id()));
                    }

                    break;

                case "gossip":

                    Tag latestTag = receivedMessage.getTag();
                    if (tagViewMap.containsKey(latestTag) ) {
                        tagViewMap.get(latestTag).setLabel(View.Label.FIN);
                        System.out.println("Updating view for already known tag");
                    }
                    else
                    {
                        System.out.println("Received gossip with an unknown tag");
                        View tmp = new View("");
                        tmp.setLabel(View.Label.FIN);
                        tagViewMap.put(latestTag, tmp);
                    }

                    //saving proper message in lastDeliveredMessageMap map, in this way I can retrieve the node view
                    lastDeliveredMessageMap.put(receivedMessage.getSenderId(), receivedMessage);
                    break;
                //Reading -1
                default:
                    System.out.println("Server or reader/writer crashed " +receivedMessage.getRequestType());

            }


    }





    //Networking functions**********************************************************************************************

    /* Accept a connection on the listening socket (remember, the local port in which messages will be sent/received will change in a transparent way */
    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        //I changed it to op_write from OP_READ because the paper states that POSSIBLY the node with higher ID should write first. If I'm accepting a connection it means my ID is higher
        socketChannel.register(selector, SelectionKey.OP_READ);
        //adding to the list of connected servers
        connectedServerChannels.add(socketChannel);
        //and updating channels of communicate
        comm.setChan(connectedServerChannels);

        System.out.println("Client " +socketChannel.getRemoteAddress().toString()+  " connected");
    }

    /* method for establishing connection to other server */
    private void finishConnection(SelectionKey key) throws IOException {

        SocketChannel socketChannel = (SocketChannel) key.channel();
        SocketAddress remote = socketChannel.getRemoteAddress();
        try {

            /* From the documentation of finishConnect()
            Finishes the process of connecting a socket channel.
            A non-blocking connection operation is initiated by placing a socket channel in non-blocking mode and then invoking its connect method.
            Once the connection is established, or the attempt has failed, the socket channel will become connectable and this method may be invoked to complete the connection sequence.
            If the connection operation failed then invoking this method will cause an appropriate IOException to be thrown.
            If this channel is already connected then this method will not block and will immediately return true.
            If this channel is in non-blocking mode then this method will return false if the connection process is not yet complete.
            If this channel is in blocking mode then this method will block until the connection either completes or fails, and will always either return true or throw a checked exception describing the failure.
            This method may be invoked at any time. If a read or write operation upon this channel is invoked while an invocation of this method is in progress then that operation will first block until this invocation is complete.
            If a connection attempt fails, that is, if an invocation of this method throws a checked exception, then the channel will be closed.
            */

            socketChannel.finishConnect();


        }catch (ConnectException e){
            //If I get this exception it means that the channel doesn't exist (the client is not up)
            reRegisterKey(remote);
            return;
        }

        /* If I'm here finishConnect didn't throw an exception so the channel should be up (and connectable/connected) */

        try {
            if (!connectedServerChannels.contains(socketChannel)) {

                // If the channel is not in our list of connected sockets, finish the connection.
                // If the connection operation failed this will raise an IOException.

                /* Here I'm sure the channel is connected so I can add it to the list of connected sockets */
                if (socketChannel.isConnected()) {

                    connectedServerChannels.add(socketChannel);
                    //Removing it from the list of not connected nodes
                    nodesToConnect.remove(socketChannel.getRemoteAddress());
                    //update view as soon as I see an active connection

                    socketChannel.register(selector, SelectionKey.OP_READ);

                    for (Integer id : allNodesAddress.keySet()) {

                        //Yes no maybe? xD
                        if (allNodesAddress.get(id).equals(socketChannel.getRemoteAddress())) {
                            System.out.println("Detected connection from:" + id + ", updating FD");
                            n.getFD().updateFDForNode(id);
                            //Node found. exiting
                            break;
                        }
                    }
                }
                //Updating communicate channels by checking the differences
                comm.setChan(connectedServerChannels);
            }
            else
                //I already know this node is connected so I just return
                return;


    } catch (IOException e){
            //need this to catch the case in which connect is called on a not alive server
            //removes the channel if it is present
            //re-register for the next connection
            /*  Don't need anything of this, to re-register a channel is sufficient to add it to the nodesToConnect list.
                If a read/write fails we add the channel who failed to that list
            removeChannelFromList(socketChannel);
            removeChannelFromComm(socketChannel);
            reRegisterKey(socketChannel);*/
            System.out.println("Finish connection crashed");
            return;
        }
    }


    /* Close all the channels of from this node to the sockets */
    public boolean stop() {

        System.out.println("Stopping the node");
        if (selector != null) {
            try {
                selector.close();
                for (SocketChannel s : connectedServerChannels) {
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

    /* Reads the nodes address and id from file in the form -> 127.0.0.1:5000 TODO: CHANGE THIS METHOD IF THE FILE CHANGES */
    private boolean readAddressesFromFile(List<String> linesToParse, ArrayList<Integer> idNodesArray){

        // for each line if it's not my port I'll add the address to the array of addresses
        for (int i = 0; i < linesToParse.size(); i++) {

            //EMULAB strings are being removed since we won't use it anymore
            String line = linesToParse.get(i);
            System.out.println(line);
            //position 0 address, 1 port/id
            String[] tokens = line.split(":");

            //filling an array with all the ids to pass it to FD and a map with id-address that may be useful later on
            idNodesArray.add(Integer.parseInt(tokens[1]));
            allNodesAddress.put(Integer.parseInt(tokens[1]), new InetSocketAddress(tokens[0], Integer.parseInt(tokens[1])));
            //allNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1]))); //local

            //String[] portTokens = tokens[1].split(":"); //local
            //ignoring smaller ids, they will connect to my address and will do accept on them
            if (Integer.parseInt(tokens[1]) < n.getSettings().getNodeId())
                continue;

            //this is me
            if (Integer.parseInt(tokens[1]) == n.getSettings().getNodeId())
                hostAddress = new InetSocketAddress(tokens[0],n.getSettings().getNodeId());
                //hostAddress = new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
            else {
                //id is bigger than my id, so I have to register as waiting to connect to them
                nodesToConnect.add(new InetSocketAddress(tokens[0],Integer.parseInt(tokens[1])));
                //nodesToConnect.add(new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
            }
        }
        return true;

    }



    //Algorithm**************************************************************************************************************************

    public void startElectionRoutine(){

        System.out.println("Last delivered message map contains: " + lastDeliveredMessageMap.keySet().toString());
        electMasterService election = new electMasterService(n);
        int leader = election.electMaster();
        n.getFD().setLeader_id(leader);
        System.out.println("Master elected with id: "+ n.getFD().getLeader_id());
            if (leader == INVALID){
                System.out.println("No suitable leader, querying...");
                read();
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

    /* Syncin the replica object of the node accordingly to the received message at the moment it ignores round number */
    public void syncReplica( Message rcvMessage, boolean isProposedView)
    {
        MachineStateReplica selectedReplica = rep.get(rcvMessage.getSenderId());
        Message lastMulticastMsg = selectedReplica.getInput();

        System.out.println("Syncing replica for node: " + rcvMessage.getSenderId());
        selectedReplica.setId(rcvMessage.getSenderId());

        System.out.println("Status: " + rcvMessage.getView().getStatus().toString());
        selectedReplica.setStatus(rcvMessage.getView().getStatus());

        System.out.println("Leader node id: "+ rcvMessage.getLeaderId());
        selectedReplica.setLeaderId(rcvMessage.getLeaderId());

        /* TODO: add rnd: if rnd == 0 -> installing, so view can be different (should be) else multicast ence views MUST be equal*/
        if (rcvMessage.getView().getStatus() == Node.Status.MULTICAST) {
            System.out.println("Storing input as last multicast message: checking virtual synchrony propery");
            selectedReplica.setInput(rcvMessage);
            if (lastMulticastMsg.getView().getValue().equals(rcvMessage.getView().getValue()) /*&& rnd != 0 */)
                System.out.println("Virtual Synchrony preserved");
            else
                System.out.println("Virtual Synchrony not preserved");
        }
        selectedReplica.setLastMessage(rcvMessage);
        System.out.println("Storing current message");

        if (isProposedView) {
            selectedReplica.setPropView(rcvMessage.getView());
            System.out.println("Storing proposed view");
        }
        else {
            selectedReplica.setView(rcvMessage.getView());
            System.out.println("Storing view");
        }
        System.out.println("Syncing completed");
    }




    //Utilities*******************************************************************************************************************

    /* finds the highest tag in tagViewMap map */
    private Tag findMaxTagFromSet(Map<Tag, View> map) {

        //the minimum tag that we have is the localTag (if it is a valid view, otherwise it is the smallest possible tag)
        Tag maxTag;
        if (n.getLocalView().getLabel() == View.Label.FIN)
            maxTag = n.getLocalTag();
        else
            maxTag = new Tag(new Epoch(0,0),0);

        Set<Tag> tags = map.keySet();
        for (Tag tag : tags) {
            if (map.get(tag).getLabel() == View.Label.PRE)
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
        Epoch ep = lastTag.getEpoch();
        //if counter is exhausted I start with a new label and set new counter to 0
        if (lastTag.isExhausted()) {
            ep.incrementEpoch();
            return new Tag(ep, 0);
        }

        //counter not exhausted so just adding the new value, first 2 lines should be useless
        lastTag.setEpoch(ep);
        int newCVal = (int) ((lastTag.getCounters().getFirst().getCounter())+1);
        lastTag.addCounter(new Counter(id,newCVal));

        return lastTag;
    }


    /* writes message into buffer */
    private void sendMessage(SocketChannel s, Message m) {

        SocketAddress remoteAdd=null;
        try {
        remoteAdd =  s.getRemoteAddress();
        System.out.println("Trying to send: " + m.getRequestType() + " to " +remoteAdd.toString());

        if (s.isConnected()){

            System.out.println("Sending: " + m.getRequestType() + " to " + remoteAdd.toString());
            writeBuffer.clear();
            writeBuffer.put(ED.encode(m).getBytes());
            writeBuffer.flip();

            while (writeBuffer.hasRemaining())
                    s.write(writeBuffer); // writing messsage to server

            writeBuffer.clear();
            }

        }  catch (IOException e) {
            try {
                s.close();
            } catch (IOException e1) {
                System.out.println("Cannot close channel in failed write");
                e1.printStackTrace();
            }
            System.out.println("Error in writing message to: "+remoteAdd);
            reRegisterKey(remoteAdd);
            connectedServerChannels.remove(s);
            comm.setChan(connectedServerChannels);
        }


    }
    /* Reads all the message in the buffer, if an exception is thrown it proceed to re-register the key or just exits */
    private String[] readMessages(SelectionKey key) {

        String message = "";
        SocketAddress remote = null;

        SocketChannel channel = (SocketChannel) key.channel();

        try{
            remote = channel.getRemoteAddress();
            readBuffer.clear();


            while (channel.read(readBuffer) > 0) {
                // flip the buffer to start reading
                readBuffer.flip();
                message += Charset.defaultCharset().decode(readBuffer);
            }

        } catch (IOException e) {
            try {
                channel.close();
            } catch (IOException e1) {
                System.out.println("Cannot close channel in failed read");
                e1.printStackTrace();
            }
            System.out.println("Server or reader/writer crashed in read");
            reRegisterKey(remote);
            connectedServerChannels.remove(key.channel());
            comm.setChan(connectedServerChannels);
        }

        return message.split("&");

    }

    //Da usare? non credo
    void removeChannelFromList(SocketChannel toRemove){

        if (connectedServerChannels.contains(toRemove))
            connectedServerChannels.remove(toRemove);

    }

    void removeChannelFromComm(SocketChannel toRemove) {

        if (comm.getChan().contains(toRemove))
            comm.getChan().remove(toRemove);
    }

    /* This function takes the remote address of the closed/crashed/invalid channel and re-register it (if the id is greater than ours) */
    void reRegisterKey(SocketAddress s){

        //System.out.println("Channel was closed... Trying to reconnect->" + s.toString());
        Set<Integer> allID = allNodesAddress.keySet();
        for (int id : allID)
        {
            SocketAddress current = allNodesAddress.get(id);
            if (current == s) {
                /* If the Id of the crashed node is greater than me I proceed with re-registering */
                if (id > n.getSettings().getNodeId()) {
                    System.out.println(id + " crashed. Registering key...");
                    break;
                }
                else
                /* Otherwise I has a smaller ID, so when it reconnects I will accept it */
                {
                    System.out.println(id + " crashed. Continue...");
                    return;
                }
            }

        }

        try {

            SocketChannel newChannel = SocketChannel.open();
            newChannel.configureBlocking(false);
            newChannel.connect(s);
            //System.out.println("Waiting " + s.toString() + " for accept");
            SelectionKey key = newChannel.register(selector, SelectionKey.OP_CONNECT);

        } catch (IOException e) {
            System.out.println("Cannot re-register channel, cannot open a new one");
            e.printStackTrace();
        }


    }

    public Set<Tag> GetAllStoredTags()
    {
        return tagViewMap.keySet();
    }

    public Collection<View> GetAllStoredViews()
    {
        return tagViewMap.values();
    }

    public Collection<Message> GetAllStoredMessages()
    {
        return lastDeliveredMessageMap.values();
    }

    /* Getters and Setters */
    public ArrayList<SocketChannel> getConnectedServerChannels() {
        return connectedServerChannels;
    }

    public void setConnectedServerChannels(ArrayList<SocketChannel> connectedServerChannels) {
        this.connectedServerChannels = connectedServerChannels;
    }

    public Map<Integer, Message> getLastDeliveredMessageMap() {
        return lastDeliveredMessageMap;
    }

    public void setLastDeliveredMessageMap(Map<Integer, Message> lastDeliveredMessageMap) {
        this.lastDeliveredMessageMap = lastDeliveredMessageMap;
    }

    public Node.State getState() {
        return state;
    }

    public void setState(Node.State state) {
        this.state = state;
    }

    public Map<Integer, MachineStateReplica> getRep() {
        return rep;
    }
    public void setRep(Map<Integer, MachineStateReplica> rep) {
        this.rep = rep;
    }

    public int getRnd() {
        return rnd;
    }

    public void setRnd(int rnd) {
        this.rnd = rnd;
    }



}
