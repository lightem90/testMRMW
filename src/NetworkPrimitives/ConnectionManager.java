package NetworkPrimitives;

import EncoderDecoder.EncDec;
import Structures.Counter;
import Structures.Message;
import Structures.Tag;
import Structures.View;
import com.robustMRMW.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by Matteo on 22/07/2015.
 */
public class ConnectionManager {

    //Class constants
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
    private ArrayList<InetSocketAddress> otherNodesAddress;
    private Map<Tag, View> rep;
    private Map<Integer,Message> replica;

    private LinkedList<Message> messageList;

    // Custom objects
    private Communicate comm;
    private Node n;
    private EncDec ED;


    // Initialization
    public ConnectionManager(Node c){


        System.out.println("Setting up node with id: " + c.getMySett().getNodeId() + " and port: " + c.getMySett().getPort());
        hostAddress = new InetSocketAddress(ADDRESS,c.getMySett().getPort());
        rep = new HashMap<>();
        ED = new EncDec();
        n = c;

        //initializing rep with first tag and current view (nobody active), the view will change as soon as we receive messages from other nodes
        rep.put(n.getLocalTag(),n.getLocalView());


    }

    /* initialize and bind selector to current address / port */
    public ArrayList<Integer> init() {

        otherNodesAddress = new ArrayList<InetSocketAddress>();
        serverChannels = new ArrayList<>();

        System.out.println("Initializing server node");

        try {

            Selector socketSelector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(hostAddress);
            hostAddress = serverChannel.getLocalAddress();
            serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
            selector = socketSelector;

            // writing local address to file
            Path filePath = Paths.get(ADDRESS_PATH);
            String toWrite = String.format(n.getMySett().getNodeId() + " " + serverChannel.getLocalAddress()
                    + "%n");
            System.out.println("The address of this node is: " + toWrite);

            // writing the address of the new node on file so every other  node can connect to it
            // TODO: should not write to file cause we create it manually or in the script?
            Files.write(filePath, toWrite.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }


        ArrayList<Integer> ids = new ArrayList<>();
        ids.add(n.getMySett().getNodeId());

        try {
            int i;
            // read addresses from file
            Path filePath = Paths.get(ADDRESS_PATH);
            List<String> lines = Files.readAllLines(filePath);
            ids = new ArrayList<>(lines.size());

            // for each line if it's not my port I'll add the address to the array of addresses
            for (i = 0; i < lines.size(); i++) {

                String line = lines.get(i);
                String[] tokens = line.split(" ");


                if (!tokens[1].equals(hostAddress.toString())) {
                    otherNodesAddress.add(getAddressFromString(tokens[1]));
                    System.out.println("Adding node with id to Failure Detector: " + tokens[0]);
                    ids.add(Integer.parseInt(tokens[0]));
                }
            }

            //initializing failure detector with data read from file



        } catch (IOException e) {
            e.printStackTrace();
        }
        return ids;
    }

    /* Connecting to all other nodes */
    public void connect() throws IOException {

        System.out.println(n.getMySett().getNodeId() + " Connecting to existing nodes");


        for (InetSocketAddress address : otherNodesAddress) {

            SocketChannel channelToAdd = SocketChannel.open();
            channelToAdd.configureBlocking(false);

            SocketAddress toAdd = address;
            channelToAdd.connect(toAdd);

            SelectionKey key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);
            serverCount++;
            serverChannels.add(channelToAdd);

            if(channelToAdd.isConnectionPending())
                channelToAdd.finishConnect();

        }



        //initializing communicate
        comm = new NetworkPrimitives.Communicate(n,this);

        Message init = new Message("init", new Tag(-1,-1,-1), new View(""), n.getMySett().getNodeId());
        for (SocketChannel ch : serverChannels)
            sendMessage(ch,init);


    }

    /*Method to wait that at least n/2+1 nodes are online */
    public void waitForQuorum(){

        System.out.println("Waiting for connections...");

        while (n.getFD().getActiveNodes().size() < n.getQuorum()){

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
                        handleInit(key);
                    } else if (key.isConnectable()) {
                        finishConnection(key);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }





        }



    }

    private void handleInit(SelectionKey key) throws IOException {

        readBuffer.clear();
        int count = 0;
        String message = "";
        SocketChannel channel = (SocketChannel) key.channel();
        Message receivedMessage = new Message();
        try {
            while (channel.read(readBuffer) > 0) {
                // flip the buffer to start reading
                readBuffer.flip();
                message += Charset.defaultCharset().decode(readBuffer);
            }

            String[] tokens = message.split("&");

            for(String msg : tokens) {

                receivedMessage = ED.decode(msg);
                System.out.println("Received " + receivedMessage.getRequestType() + " from node #" + receivedMessage.getSenderId());

                switch (receivedMessage.getRequestType()) {
                    //New messages to handle new server connection, init_ack is used only to update client FD, init connects the new client and updates communicate obj (chans, serverNumber etc..)
                    case "init":
                        //update FD, serverChannels and serverCount
                        handleConnectionRequest(key,receivedMessage);
                        n.getFD().updateFDForNode(receivedMessage.getSenderId());
                        comm = new Communicate(n,this);
                        break;

                    case "init_ack":
                        n.getFD().updateFDForNode(receivedMessage.getSenderId());
                        break;
                }
            }
        }
        catch (IOException e) {
            System.out.println("Server crashed, keep executing");
            channel.close();
            key.cancel();
            return;
        }


    }

    /* this handles the case in which we receive a init message and we have to update the list of nodes addresses, channels and FD (and answering) */
    private void handleConnectionRequest(SelectionKey k, Message m){

        Path filePath = Paths.get(ADDRESS_PATH);
        List<String> lines = null;
        try {

            lines = Files.readAllLines(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // for each line if it's not my port I'll add the address to the array of addresses
        for (int i = 0; i < lines.size(); i++) {

            String line = lines.get(i);
            String[] tokens = line.split(" ");
            if (m.getSenderId() == Integer.parseInt(tokens[0])) {
                if (!otherNodesAddress.contains(tokens[1]))
                    otherNodesAddress.add(getAddressFromString(tokens[1]));


                SocketChannel channelToAdd = null;
                try {

                    channelToAdd = SocketChannel.open();
                    channelToAdd.configureBlocking(false);
                    channelToAdd.connect(getAddressFromString(tokens[1]));


                    try {
                        SelectionKey key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);
                    } catch (ClosedChannelException e) {
                        e.printStackTrace();
                    }

                    serverCount++;
                    n.setQuorum(serverCount/2);
                    serverChannels.add(channelToAdd);


                    if(channelToAdd.isConnectionPending())
                        channelToAdd.finishConnect();


                    Message init = new Message("init_ack", new Tag(-1,-1,-1), new View(""), n.getMySett().getNodeId());
                    sendMessage(channelToAdd,init);

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

        }


    }


    /* Servicing method, starts one thread for servicing requests and one for user input  */
    public void run() {

        System.out.println("Running ...");
        replica = new HashMap<>();

        while (true) {
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
                        System.out.println("IS READABLE");
                        parseInput(key);
                    } else if (key.isConnectable()) {
                        finishConnection(key);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void parseInput(SelectionKey key) throws IOException {

        readBuffer.clear();
        int count = 0;
        String message = "";
        SocketChannel channel = (SocketChannel) key.channel();
        Message receivedMessage = new Message();
        try {
            while (channel.read(readBuffer) > 0) {
                // flip the buffer to start reading
                readBuffer.flip();
                message += Charset.defaultCharset().decode(readBuffer);
            }




            String[] tokens = message.split("&");

            for(String msg : tokens) {

                receivedMessage = ED.decode(msg);

                System.out.println("Received query '"
                        + receivedMessage.getRequestType() + "' from node #"
                        + receivedMessage.getSenderId());
                Tag maxTag = findMaxTagFromSet(rep);

                switch (receivedMessage.getRequestType()) {

                    case "query":
                        sendMessage(channel, new Message("query-ack", maxTag, rep.get(maxTag), n.getMySett().getNodeId()));
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
                        sendMessage(channel, new Message("pre-write-ack", n.getLocalTag(),
                                n.getLocalView(), n.getMySett().getNodeId()));
                        break;

                    case "finalize":
                        //saving proper message in replica map, in this way I can retrieve the node view
                        replica.put(receivedMessage.getSenderId(),receivedMessage);

                        Tag bestTag = receivedMessage.getTag();
                        //System.out.println("Received tag has: label->" + bestTag.getLabel() + " counter->" + bestTag.getCounters().getFirst().getCounter() + " written by->" + bestTag.getCounters().getFirst().getId());
                        //System.out.println("Local tag has: label->" + n.getLocalTag().getLabel() + " counter->" + n.getLocalTag().getCounters().getFirst().getCounter() + " written by->" + n.getLocalTag().getCounters().getFirst().getId());

                        if (rep.containsKey(bestTag)) {
                            rep.get(bestTag).setStatus(View.Status.FIN);
                            sendMessage(channel, new Message("fin-ack", n.getLocalTag(),
                                    n.getLocalView(), n.getMySett().getNodeId()));
                        } else {

                            //TODO: had to had a response, otherwise no ack was sent and couldn't go forward
                            //if we are here the tag we received is not contained in our rep map (can happen). Check the paper on drive
                            View tmp = new View("");
                            tmp.setStatus(View.Status.FIN);
                            rep.put(bestTag, tmp);
                            sendMessage(channel, new Message("fin-ack", bestTag,
                                    tmp, n.getMySett().getNodeId()));
                        }
                        n.getLocalView().setStatus(View.Status.FIN);

                        break;

                    case "gossip":
                        //saving proper message in replica map, in this way I can retrieve the node view
                        replica.put(receivedMessage.getSenderId(),receivedMessage);

                        Tag latestTag = receivedMessage.getTag();
                        if (rep.containsKey(latestTag))
                            rep.put(latestTag, receivedMessage.getView());
                        break;

                    case "userReadRequest":
                        read();
                        sendMessage(channel,
                                new Message("success", n.getLocalTag(),
                                        n.getLocalView(), n.getMySett().getNodeId()));
                        break;

                    case "userWriteRequest":
                        write(receivedMessage.getView());
                        System.out.println("Replying to request with result: success");
                        sendMessage(channel,
                                new Message("success", n.getLocalTag(),
                                        n.getLocalView(), n.getMySett().getNodeId()));
                        break;

                }
            }
        } catch (IOException e) {
            System.out.println("Server or reader/writer crashed, keep executing");

            channel.close();
            key.cancel();
            return;
        }
    }




    //Networking functions

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

        writeBuffer.clear();
        writeBuffer.put(ED.encode(m).getBytes());
        writeBuffer.flip();

        while (writeBuffer.hasRemaining())
            s.write(writeBuffer); // writing messsage to server
        writeBuffer.clear();

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
