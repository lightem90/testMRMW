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
    private Map<Tag, View> rep;
    private Map<Integer,Message> replica;

    // Custom objects
    private Communicate comm;
    private Node n;
    private EncDec ED;


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

                String line = lines.get(i);
                System.out.println(line);
                String[] tokens = line.split(" ");

                //filling an array with all the ids to pass it to FD and a map with id-address that may be useful later on
                ids.add(Integer.parseInt(tokens[0]));
                otherNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(tokens[1],Integer.parseInt(tokens[0])));
                //otherNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1]))); //local

                //String[] portTokens = tokens[1].split(":"); //local
                //ignoring smaller ids
                if (Integer.parseInt(tokens[0]) <= n.getSettings().getNodeId())
                    continue;

                if (Integer.parseInt(tokens[0]) == n.getSettings().getNodeId())
                    hostAddress = new InetSocketAddress(tokens[1],n.getSettings().getNodeId());
                   //hostAddress = new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
                else {
                    //id is bigger than my id
                    nodesToConnect.add(new InetSocketAddress(tokens[1],Integer.parseInt(tokens[0])));
                   //nodesToConnect.add(new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
                }
                /* EMULAB
                //port is handled differently on EMULAB, lines with "local" next to them are for local lines with "emulab" are for emulab
                String[] portTokens = tokens[1].split(":"); //local

                if (Integer.parseInt(tokens[0]) == n.getSettings().getNodeId())
                    //hostAddress = new InetSocketAddress(tokens[1],PORT); //emulab
                    hostAddress = new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1])); //local
                else
                    //otherNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(tokens[1],PORT)); //emulab
                    otherNodesAddress.put(Integer.parseInt(tokens[0]), new InetSocketAddress(portTokens[0],Integer.parseInt(portTokens[1]))); //local
                    */
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

                        for (String msg : tokens) {


                            Message m = ED.decode(msg);
                            System.out.println("Received " + m.getRequestType() + " from node #" + m.getSenderId());
                            //ignoring invalid messages (sender -1 is invalid)
                            if (m.getSenderId() == -1)
                                continue;
                            n.getFD().updateFDForNode(m.getSenderId());

                            //Handling init messages first, in this way I shouldn't have problem with the others messages
                            if (handleInit(m, (SocketChannel) key.channel()))
                                parseInput(m, (SocketChannel) key.channel());

                            /* Used for testing
                            if (n.getSettings().getNodeId() == 5000)
                                write(n.getLocalView());
                                */

                            //This must start as soon as we know a quorum is present
                            //if (n.getFD().getActiveNodes().size() >= n.getSettings().getQuorum() && n.getFD().getLeader_id() == -1)
                            //    startElectionRoutine();
                        }


                    } else if (key.isConnectable())
                        finishConnection(key);

                    }


            } catch (Exception e) {
                e.printStackTrace();
            }


    }

    private boolean handleInit(Message receivedMessage, SocketChannel channel) throws IOException {

        switch (receivedMessage.getRequestType()) {
                    /*New messages to handle new server connection, init_ack is used only to update client FD, init connects the new client and updates communicate obj (chans, serverNumber etc..) */
                case "init":
                    //update serverChannels and serverCount
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    //handleConnectionRequest("init_ack",receivedMessage.getSenderId()); We don't need this anymore
                    int l_id = n.getFD().getLeader_id();
                    Tag leader = new Tag(l_id, l_id, l_id);
                    //sending as answer my local view (all the nodes my fd says are active
                    Message response = new Message("init_ack", leader, n.getLocalView(), n.getSettings().getNodeId());
                    sendMessage(channel,response);
                    return true;

                case "init_ack":

                    if (receivedMessage.getTag().getId() == -1 && receivedMessage.getTag().getLabel() == -1)
                        //no leader is present, just warning
                        System.out.println("No leader present in the system");
                    else {
                        System.out.println("Leader is: " + receivedMessage.getTag().getLabel());
                        n.getFD().setLeader_id(receivedMessage.getTag().getLabel());
                    }
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    return true;
                default:
                    return false;
        }
    }


    private void parseInput(Message receivedMessage, SocketChannel channel) throws IOException {

        Tag maxTag = findMaxTagFromSet(rep);

            switch (receivedMessage.getRequestType()) {

                case "query":
                    sendMessage(channel, new Message("query-ack", maxTag, rep.get(maxTag), n.getSettings().getNodeId()));
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    break;

                case "pre-write":
                    Tag newTag = receivedMessage.getTag();

                    System.out.println("Received tag has: label->" + newTag.getLabel() + " counter->" + newTag.getCounters().getFirst().getCounter() + " written by->" + newTag.getCounters().getFirst().getId());
                    System.out.println("Local tag has: label->" + n.getLocalTag().getLabel() + " counter->" + n.getLocalTag().getCounters().getFirst().getCounter() + " written by->" + n.getLocalTag().getCounters().getFirst().getId());

                    if (maxTag.compareTo(newTag) >= 0) {
                        System.out.println("Received tag smaller than local max tag");
                        sendMessage(channel, new Message("pre-write-ack", new Tag(-1,-1,-1), new View (""), n.getSettings().getNodeId()));
                        break;
                    }
                    else {

                        //only master writes
                        //n.getFD().setLeader_id(receivedMessage.getSenderId());

                        n.setLocalTag(newTag);
                        n.setLocalView(receivedMessage.getView());
                        n.getLocalView().setStatus(View.Status.PRE);
                        rep.put(n.getLocalTag(), n.getLocalView());
                        sendMessage(channel, new Message("pre-write-ack", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId()));
                    }
                    break;

                case "finalize":
                    //saving proper message in replica map, in this way I can retrieve the node view NO! invalid view (yet)
                    //replica.put(receivedMessage.getSenderId(), receivedMessage);
                    Tag bestTag = receivedMessage.getTag();
                    //System.out.println("Received tag has: label->" + bestTag.getLabel() + " counter->" + bestTag.getCounters().getFirst().getCounter() + " written by->" + bestTag.getCounters().getFirst().getId());
                    //System.out.println("Local tag has: label->" + n.getLocalTag().getLabel() + " counter->" + n.getLocalTag().getCounters().getFirst().getCounter() + " written by->" + n.getLocalTag().getCounters().getFirst().getId());

                    if (rep.containsKey(bestTag)) {
                        rep.get(bestTag).setStatus(View.Status.FIN);
                        sendMessage(channel, new Message("fin-ack", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId()));
                    } else {

                        View tmp = new View("");
                        tmp.setStatus(View.Status.FIN);
                        rep.put(bestTag, tmp);
                        sendMessage(channel, new Message("fin-ack", bestTag, tmp, n.getSettings().getNodeId()));
                    }
                    n.getLocalView().setStatus(View.Status.FIN);
                    break;

                case "gossip":
                    //saving proper message in replica map, in this way I can retrieve the node view
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    Tag latestTag = receivedMessage.getTag();
                    if (rep.containsKey(latestTag))
                        rep.put(latestTag, receivedMessage.getView());
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    break;
                /*
                case "userReadRequest":
                    read();
                    sendMessage(channel, new Message("success", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId()));
                    break;

                case "userWriteRequest":
                    write(receivedMessage.getView());
                    System.out.println("Replying to request with result: success");
                    sendMessage(channel, new Message("success", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId()));
                    break;
                    */
                //Reading -1
                default:
                    System.out.println("Server or reader/writer crashed " +receivedMessage.getRequestType());
                    if (serverChannels.contains(channel))
                        serverChannels.remove(channel);
                    channel.close();
                    comm.setChan(serverChannels);
            }


    }





    //Networking functions

    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
        //add the accepted socket to the list of active servers
        serverChannels.add(socketChannel);

        System.out.println("Client connected");
    }

    // if a read request arises from selector
    private void read() {

        Tag maxTag;
        if ((maxTag = comm.query()) == null)
            return;
        System.out.println("Received tag after query has: label->" + maxTag.getLabel() + " counter->" + maxTag.getCounters().getFirst().getCounter() + " written by->" + maxTag.getCounters().getFirst().getId());
        View rcvView = comm.finalizeRead();

        n.setLocalTag(maxTag);
        n.setLocalView(rcvView);
        for (SocketChannel sc : serverChannels) {
            try {
                sendMessage(sc, new Message("gossip", maxTag, rcvView, n.getSettings().getNodeId()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void write(View newView) {

        long tStart = System.currentTimeMillis();

        Tag maxTag;
        if ((maxTag = comm.query()) == null)
            return;

        System.out.println("Received tag after query has: label->" + maxTag.getLabel() + " counter->" + maxTag.getCounters().getFirst().getCounter() + " written by->" + maxTag.getCounters().getFirst().getId());
        Tag newTag = generateNewTag(maxTag);
        System.out.println("Writing new tag: label->" + newTag.getLabel() + " counter->" + newTag.getCounters().getFirst().getCounter() + " written by->" + newTag.getCounters().getFirst().getId());

        if (comm.preWrite(newTag, newView)) {
            n.setLocalTag(newTag);
            n.setLocalView(newView);
            rep.put(newTag, newView);
            if (comm.finalizeWrite())
                System.out.println("Write complete correctly");
            else
                System.out.println("Write error");

            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            double elapsedSeconds = tDelta / 1000.0;

            System.out.println("Elapsed time for write: " + elapsedSeconds);
        }
        else {
            System.out.println("Can't complete pre-write - aborting");
            return;
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

                        if (otherNodesAddress.get(id).equals(socketChannel.getRemoteAddress())) {
                            System.out.println("Detected connection from:" + id + ", updating FD");
                            n.getFD().updateFDForNode(id);
                        }
                    }
                }

                Message init = new Message("init", n.getLocalTag(), n.getLocalView(), n.getSettings().getNodeId());
                sendMessage(socketChannel, init);

                //initializing communicate, needed here to keep up of new connection. TODO: What happens to old channels when I do setChan to older chans in communicate?
                comm.setChan(serverChannels);

            } catch (IOException e) {

                removeChannelFromList(socketChannel);
                removeChannelFromComm(socketChannel);
                socketChannel.close();
                key.cancel();
                return;
            }
        }
    }

    private void startElectionRoutine(){

        System.out.println("This replica (CM) contains: " + replica.keySet().toString());
        electMasterService election = new electMasterService(n.getSettings(), n.getLocalView(), this, n.getFD().getActiveNodes());
        int leader = election.electMaster();
        n.getFD().setLeader_id(leader);
        System.out.println("Master elected with id: "+ n.getFD().getLeader_id());
            if (leader == -1){
                System.out.println("No suitable leader, querying...");
                read();
                System.out.println("Query ended");
                return;

            }
            if (leader == n.getSettings().getNodeId()) {
                n.setIsMaster(true);
                write(n.getLocalView());
            }

    }




    //Utilities

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
    private void sendMessage(SocketChannel s, Message m) throws IOException {

        System.out.println("Trying to send: " + m.getRequestType() + " to " + s.getRemoteAddress().toString());

        if (s.isConnected()){

            System.out.println("Sending: " + m.getRequestType() + " to " + s.getRemoteAddress().toString());
            writeBuffer.clear();
            writeBuffer.put(ED.encode(m).getBytes());
            writeBuffer.flip();

            while (writeBuffer.hasRemaining())
                s.write(writeBuffer); // writing messsage to server
            writeBuffer.clear();
        }
        else {
            System.out.println("Channel not connected, can't send message: " +m.getRequestType());

        }


    }

    private String[] readMessages(SelectionKey key) throws IOException {

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


    /* Needed to update the replica message: we imply that if we receive a init message all the other nodes receive the same message too */
    /*private void updateRep (int id) {

        if (id != -1){

        Iterator<Integer> i = replica.keySet().iterator();

        if (replica.size() > 0) {
            while (i.hasNext()) {
                Message m = replica.get(i.next());

                if (m.getSenderId() != id) {

                    System.out.println("Received init from: " + id + " updating view of node: " + m.getSenderId());
                    View v = m.getView();
                    v.setArrayFromValueString();
                    ArrayList<Integer> ids = v.getIdArray();
                    System.out.println("Old view: " + v.getValue());
                    if (!ids.contains(id))
                        ids.add(id);
                    v.setIdArray(ids);
                    v.setStringFromArrayString();
                    System.out.println("New view: " + v.getValue());
                    m.setView(v);
                    replica.put(id, m);

                }


            }
        }
        }
    }
    */




    /* this handles the case in which we receive a init message and we have to update the list of nodes addresses, channels and FD (and answering) */
    /*private void handleConnectionRequest(String type, int id){

        //Getting the id
        SocketAddress toAdd = null;
        SocketChannel channelToAdd = null;

        if (otherNodesAddress.containsKey(id) && (id != n.getSettings().getNodeId()))
            toAdd = otherNodesAddress.get(id);
        else {
            System.out.println("Cannot detect newly connected node");
            return;
        }

        try {

            //no checks there shouldn't be problems while connecting to a requesting connection node
            channelToAdd = SocketChannel.open();
            channelToAdd.configureBlocking(false);
            channelToAdd.connect(toAdd);

            while(channelToAdd.isConnectionPending())
                channelToAdd.finishConnect();
            if (!serverChannels.contains(channelToAdd))
                serverChannels.add(channelToAdd);

            //if in some way I know the leader id, I send it as ack to a newly connected node
            int l_id = n.getFD().getLeader_id();
            Tag leader = new Tag(l_id, l_id, l_id);

            //sending as answer my local view (all the nodes my fd says are active
            Message response = new Message(type, leader, n.getLocalView(), n.getSettings().getNodeId());
            sendMessage(channelToAdd,response);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    */


}
