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
    //private final static String ADDRESS_PATH = "/groups/Gulliver/virtualSynchrony/address.txt";
    private final static String ADDRESS_PATH = "address.txt";
    private final static String ADDRESS = "localhost";
    private final static int PORT = 0;


    // Class buffer
    private static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    // Class connection variables
    private int serverCount = 0;
    private SocketAddress hostAddress;
    private Selector selector;
    private ArrayList<SocketChannel> serverChannels;
    private Map<Integer,InetSocketAddress> otherNodesAddress;
    private Map<Tag, View> rep;
    private Map<Integer,Message> replica;

    // Custom objects
    private Communicate comm;
    private Node n;
    private EncDec ED;


    // Initialization
    public ConnectionManager(Node c){

        hostAddress = new InetSocketAddress(ADDRESS,c.getMySett().getPort());
        rep = new HashMap<>();
        ED = new EncDec();
        n = c;

        //initializing rep with first tag and current view (nobody active), the view will change as soon as we receive messages from other nodes
        rep.put(n.getLocalTag(),n.getLocalView());


    }

    /* initialize and bind selector to current address / port */
    public ArrayList<Integer> init() {

        otherNodesAddress = new HashMap<>();
        serverChannels = new ArrayList<>();
        replica = new HashMap<>();
        Selector socketSelector = null;

        try {

            socketSelector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(hostAddress);
            hostAddress = serverChannel.getLocalAddress();
            serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
            selector = socketSelector;

        } catch (IOException e) {

            System.out.println("Cannot initialize selector");
            e.printStackTrace();
        }

        ArrayList<Integer> ids = new ArrayList<>();

        try {
            int i;
            // read addresses from file
            Path filePath = Paths.get(ADDRESS_PATH);
            List<String> lines = Files.readAllLines(filePath);

            // for each line if it's not my port I'll add the address to the array of addresses
            for (i = 0; i < lines.size(); i++) {

                String line = lines.get(i);
                String[] tokens = line.split(" ");
                otherNodesAddress.put(Integer.parseInt(tokens[0]), getAddressFromString(tokens[1]));
                ids.add(Integer.parseInt(tokens[0]));
                serverCount = ids.size();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //returns an array with the ids of all nodes in the network (read from address.txt file). The quorum is ids.size()/2
        return ids;
    }

    /* Connecting to all other nodes */
    public void connect() throws IOException {

        Iterator i = otherNodesAddress.keySet().iterator();
        SelectionKey key = null;

        while(i.hasNext()){
            int id = (int)i.next();

            if (id != n.getMySett().getNodeId()) {

                SocketAddress toAdd = otherNodesAddress.get(id);
                SocketChannel channelToAdd = SocketChannel.open();
                channelToAdd.configureBlocking(false);
                channelToAdd.connect(toAdd);
                key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);

                if (channelToAdd.isConnectionPending())
                    channelToAdd.finishConnect();

                //adding only connected channels and storing the other channels addresses in otherNodesAddress map in this way I shouldn't read the file ever again
                if (channelToAdd.isConnected()) {
                    serverChannels.add(channelToAdd);

                }
            }

        }
        //initializing communicate
        comm = new NetworkPrimitives.Communicate(n,this);

        Message init = new Message("init", n.getLocalTag(), n.getLocalView(), n.getMySett().getNodeId());
        if (serverChannels.size() > 0) {
            for (SocketChannel ch : serverChannels)
                    sendMessage(ch, init);
            //TODO: read here = deadlock
            //read();
        }




    }


    /* Servicing method, starts one thread for servicing requests and one for user input  */
    public void run() {

            try {
                // listening for connections, initializes the selector with all
                // socket ready for I/O operations
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

                        String[] tokens;
                        try{
                            tokens = readMessages(key);
                        } catch (IOException e) {
                            key.channel().close();
                            key.cancel();
                            serverChannels.remove(key.channel());
                            System.out.println("Server or reader/writer crashed");
                            comm = new Communicate(n,this);
                            continue;
                        }
                        for(String msg : tokens) {

                            Message m = ED.decode(msg);

                                boolean wasInit = handleInit(m);

                                /*TODO: this check could become a variable */
                                if (n.getFD().getActiveNodes().size() >= n.getMySett().getQuorum()) {

                                    if (n.getFD().getLeader_id() == -1) {
                                        System.out.println("Master is not present in the system, starting leader election routine");
                                        //TODO: should I read here somewhere? Meaning that if some trouble happend during the l.e. any node should wait a read at least from a quorum (if there's still a quorum)
                                        electMasterService election = new electMasterService(n.getMySett(), n.getLocalView(), this, n.getFD().getActiveNodes());
                                        int leader = election.electMaster();
                                        n.getFD().setLeader_id(leader);
                                        if (leader == -1){
                                            System.out.println("No suitable leader, querying...");
                                            read();
                                            System.out.println("Query ended");
                                            continue;

                                        }
                                        if (leader == n.getMySett().getNodeId()) {
                                            n.setIsMaster(true);
                                            write(n.getLocalView());
                                        }
                                        System.out.println("Master elected with id: "+ n.getFD().getLeader_id());
                                    }

                                    //Master is present
                                    if(!wasInit)
                                        parseInput(m, (SocketChannel)key.channel());
                                }


                        }
                    } else if (key.isConnectable()) {
                        finishConnection(key);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }


    }

    private boolean handleInit(Message receivedMessage) throws IOException {

        System.out.println("Received " + receivedMessage.getRequestType() + " from node #" + receivedMessage.getSenderId());

        switch (receivedMessage.getRequestType()) {
                    /*New messages to handle new server connection, init_ack is used only to update client FD, init connects the new client and updates communicate obj (chans, serverNumber etc..) */
                case "init":
                    //update FD, serverChannels and serverCount
                    n.getFD().updateFDForNode(receivedMessage.getSenderId());
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    handleConnectionRequest("init_ack",receivedMessage.getSenderId());
                    updateRep(receivedMessage.getSenderId());
                    comm = new Communicate(n,this);
                    return true;

                case "init_ack":

                    if (receivedMessage.getTag().getId() == -1 && receivedMessage.getTag().getLabel() == -1)
                        //no leader is present, just warning
                        System.out.println("No leader present in the system");
                    else {
                        System.out.println("Leader is: " + receivedMessage.getTag().getLabel());
                        n.getFD().setLeader_id(receivedMessage.getTag().getLabel());
                        //TODO: what to do you received view? If there's a quorum the view should be valid
                    }

                    //Setting the leader Id from response, I don't check which value it has
                    n.getFD().setLeader_id(receivedMessage.getTag().getId());
                    n.getFD().updateFDForNode(receivedMessage.getSenderId());
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    handleConnectionRequest("end_handshake",receivedMessage.getSenderId());
                    return true;

                    /*Need this to send correct view to older node (with init I send the view with only me active */
                case "end_handshake":
                    System.out.println("Updating view for newly connected node");
                    n.getFD().updateFDForNode(receivedMessage.getSenderId());
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    return true;
                default:
                    return false;
        }
    }



    /* Needed to update the replica message: we imply that if we receive a init message all the other nodes receive the same message too */
    private void updateRep (int id) {

        Collection<Message> c = replica.values();

        if (c.size() > 0) {
            for (Message m : c) {

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



    /* this handles the case in which we receive a init message and we have to update the list of nodes addresses, channels and FD (and answering) */
    private void handleConnectionRequest(String type, int id){

        //Getting the id
        SocketAddress toAdd = null;
        SocketChannel channelToAdd = null;

        if (otherNodesAddress.containsKey(id) && (id != n.getMySett().getNodeId()))
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

            if(channelToAdd.isConnectionPending())
                channelToAdd.finishConnect();
            if (!serverChannels.contains(channelToAdd))
                serverChannels.add(channelToAdd);

            //if in some way I know the leader id, I send it as ack to a newly connected node
            int l_id = n.getFD().getLeader_id();
            Tag leader = new Tag(l_id, l_id, l_id);

            //sending as answer my local view (all the nodes my fd says are active
            Message response = new Message(type, leader, n.getLocalView(), n.getMySett().getNodeId());
            sendMessage(channelToAdd,response);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void parseInput(Message receivedMessage, SocketChannel channel) throws IOException {

        Tag maxTag = findMaxTagFromSet(rep);
        System.out.println("Received message '" + receivedMessage.getRequestType() + "' from node #" + receivedMessage.getSenderId());

            switch (receivedMessage.getRequestType()) {

                case "query":
                    sendMessage(channel, new Message("query-ack", maxTag, rep.get(maxTag), n.getMySett().getNodeId()));
                    replica.put(receivedMessage.getSenderId(),receivedMessage);
                    break;

                case "pre-write":
                    Tag newTag = receivedMessage.getTag();
                    //System.out.println("Received tag has: label->" + newTag.getLabel() + " counter->" + newTag.getCounters().getFirst().getCounter() + " written by->" + newTag.getCounters().getFirst().getId());
                    //System.out.println("Local tag has: label->" + n.getLocalTag().getLabel() + " counter->" + n.getLocalTag().getCounters().getFirst().getCounter() + " written by->" + n.getLocalTag().getCounters().getFirst().getId());

                    if (maxTag.compareTo(newTag) >= 0) {
                        System.out.println("Received tag smaller than local max tag");
                        break;
                    }

                    n.setLocalTag(newTag);
                    n.setLocalView(receivedMessage.getView());
                    n.getLocalView().setStatus(View.Status.PRE);
                    rep.put(n.getLocalTag(), n.getLocalView());
                    sendMessage(channel, new Message("pre-write-ack", n.getLocalTag(), n.getLocalView(), n.getMySett().getNodeId()));
                    break;

                case "finalize":
                    //saving proper message in replica map, in this way I can retrieve the node view
                    replica.put(receivedMessage.getSenderId(), receivedMessage);
                    Tag bestTag = receivedMessage.getTag();
                    //System.out.println("Received tag has: label->" + bestTag.getLabel() + " counter->" + bestTag.getCounters().getFirst().getCounter() + " written by->" + bestTag.getCounters().getFirst().getId());
                    //System.out.println("Local tag has: label->" + n.getLocalTag().getLabel() + " counter->" + n.getLocalTag().getCounters().getFirst().getCounter() + " written by->" + n.getLocalTag().getCounters().getFirst().getId());

                    if (rep.containsKey(bestTag)) {
                        rep.get(bestTag).setStatus(View.Status.FIN);
                        sendMessage(channel, new Message("fin-ack", n.getLocalTag(), n.getLocalView(), n.getMySett().getNodeId()));
                    } else {

                        View tmp = new View("");
                        tmp.setStatus(View.Status.FIN);
                        rep.put(bestTag, tmp);
                        sendMessage(channel, new Message("fin-ack", bestTag, tmp, n.getMySett().getNodeId()));
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

                case "userReadRequest":
                    read();
                    sendMessage(channel, new Message("success", n.getLocalTag(), n.getLocalView(), n.getMySett().getNodeId()));
                    break;

                case "userWriteRequest":
                    write(receivedMessage.getView());
                    System.out.println("Replying to request with result: success");
                    sendMessage(channel, new Message("success", n.getLocalTag(), n.getLocalView(), n.getMySett().getNodeId()));
                    break;
            }


    }





    //Networking functions

    public void operation(){


        write(n.getLocalView());

    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
                .channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);

        socketChannel.register(selector, SelectionKey.OP_READ);

        System.out.println("Client listening on local port: " + socketChannel.getLocalAddress());
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
                sendMessage(sc, new Message("gossip", maxTag, rcvView, n.getMySett().getNodeId()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // if a write request arises from selector
    private void write(View newView) {
        Tag maxTag;
        if ((maxTag = comm.query()) == null)
            return;
        System.out.println("Received tag after query has: label->" + maxTag.getLabel() + " counter->" + maxTag.getCounters().getFirst().getCounter() + " written by->" + maxTag.getCounters().getFirst().getId());
        Tag newTag = generateNewTag(maxTag);
        System.out.println("Writing new tag: label->" + newTag.getLabel() + " counter->" + newTag.getCounters().getFirst().getCounter() + " written by->" + newTag.getCounters().getFirst().getId());
        comm.preWrite(newTag, newView);
        n.setLocalTag(newTag);
        n.setLocalView(newView);
        rep.put(newTag, newView);
        if (comm.finalizeWrite())
            System.out.println("WE DID IT REDDIT");
        else
            System.out.println("WE LOST");

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

    /* method for estabilishing connection to other server */
    private void finishConnection(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // Finish the connection. If the connection operation failed
        // this will raise an IOException.
        try {
            if (socketChannel.finishConnect())
                key.cancel();
        } catch (IOException e) {
            // Cancel the channel's registration with our selector
            key.cancel();
            return;
        }
    }




    //Utilities

    private InetSocketAddress getAddressFromString(String line) {

        line = line.replace("/", "");
        line = line.replace(":", " ");
        String[] splitted = line.split(" ");
        return new InetSocketAddress(splitted[0],
                Integer.parseInt(splitted[1]));
    }


    /* finds the hidhest tag in rep map */
    private Tag findMaxTagFromSet(Map<Tag, View> map) {

        //the minimum tag that we have is the localTag (initialized to id,0,0 at the beginning)
        Tag maxTag = n.getLocalTag();

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

        int id = n.getMySett().getNodeId();
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

        System.out.println("Sending: " + m.getRequestType() + " to " + s.getRemoteAddress());

        if (s.isConnectionPending() || !s.isConnected())
            s.finishConnect();


        writeBuffer.clear();
            writeBuffer.put(ED.encode(m).getBytes());
            writeBuffer.flip();

            while (writeBuffer.hasRemaining())
                s.write(writeBuffer); // writing messsage to server
            writeBuffer.clear();


    }

    private String[] readMessages(SelectionKey key) throws IOException {


        readBuffer.clear();
        String message = "";
        SocketChannel channel = (SocketChannel) key.channel();
            while (channel.read(readBuffer) > 0) {
                // flip the buffer to start reading
                readBuffer.flip();
                message += Charset.defaultCharset().decode(readBuffer);
            }

            return message.split("&");

    }




    /* Getters and Setters */
    public ArrayList<SocketChannel> getServerChannels() {
        return serverChannels;
    }

    public void setServerChannels(ArrayList<SocketChannel> serverChannels) {
        this.serverChannels = serverChannels;
    }

    public int getServerCount() {
        return serverCount;
    }

    public void setServerCount(int serverCount) {
        this.serverCount = serverCount;
    }

    public Map<Integer, Message> getReplica() {
        return replica;
    }

    public void setReplica(Map<Integer, Message> replica) {
        this.replica = replica;
    }



}
