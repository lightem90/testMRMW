package com.robustMRMW;

import NetworkPrimitives.Communicate;
import Structures.Message;
import Structures.Tag;
import Structures.View;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Node {

    // Class constants
    private static int LIST_SIZE = 10;

    // Class variables
    private int id;
    private Communicate comm;
    private ArrayList<Integer> counter;
    private Map<Tag, View> rep;
    private ArrayList<Boolean> turns;
    private View localView;
    private View proposedView;
    private Tag localTag;
    private boolean isMaster;

    // Class lists
    private LinkedList<Message> messageList;
    private ArrayList<SocketChannel> serverChannels;

    // Class connection variables
    public final static String ADDRESS_PATH = "address.txt";
    public final static String ADDRESS = "localhost";
    public final static int PORT = 0;
    private int serverCount = 0;
    private SocketAddress hostAddress;
    private ArrayList<NodeInfo> otherNodesAddress;
    private Selector selector;

    // Class buffer
    public static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    /* Constructor with custom port */
    public Node(View view, int port) {

        // initialization with first view

        rep = new HashMap<>();
        localView = view;

        hostAddress = new InetSocketAddress(ADDRESS, port);

    }

    /* Constructor with default port */
    public Node(View view) {
        this(view, PORT);
    }

    public Node(int id, String address, int port, View view){

        this.id = id;
        hostAddress = new InetSocketAddress(address, port);
        localView = view;
        localTag = new Tag(id,0);






    }

    /* initialize and bind selector to current address / port */
    public void init() {

        System.out.println("Initializing server node");

        try {
            // Selector socketSelector =
            // SelectorProvider.provider().openSelector();
            Selector socketSelector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            // setting non blocking option
            serverChannel.configureBlocking(false);

            serverChannel.socket().bind(hostAddress);

            // in this way I retrieve and store the address and the local port
            hostAddress = serverChannel.getLocalAddress();

            serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);
            selector = socketSelector;

            // initialize communicate manager
            comm = new NetworkPrimitives.Communicate(this);

            // writing local address to file
            Path filePath = Paths.get(ADDRESS_PATH);
            String toWrite = String.format(serverChannel.getLocalAddress()
                    + "%n");
            System.out.println("The address of this node is: " + toWrite);

            // writing the address of the new node on file so every other
            // node can connect to it
            // TODO should not write to file cause we create the file manually
            // or in the script
            Files.write(filePath, toWrite.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	/* Reading and writing address and port to file */

    public void setup() {

        System.out.println("Reading addresses from file " + ADDRESS_PATH);

        otherNodesAddress = new ArrayList<>();
        serverChannels = new ArrayList<>();

        try {
            int i;
            // read addresses from file
            Path filePath = Paths.get(ADDRESS_PATH);
            List<String> lines = Files.readAllLines(filePath);

            // for each line if it's not my port I'll add the address to the
            // array of addresses
            for (i = 0; i < lines.size(); i++) {

                String line = lines.get(i);
                if (!line.equals(hostAddress.toString()))
                    otherNodesAddress.add(getAddressFromString(line, i + 1));
                else
                    setId(i + 1);
            }

            localTag = new Tag(i, 0);
            rep.put(localTag, localView);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /* Connecting to all other nodes */
    public void connect() throws IOException {

        System.out.println(id + " Connecting to existing nodes");

        for (NodeInfo address : otherNodesAddress) {

            SocketChannel channelToAdd = SocketChannel.open();
            channelToAdd.configureBlocking(false);

            SocketAddress toAdd = address.getAddress();

            // connect to server with inetadresses previously read, we assume
            // connect works
            channelToAdd.connect(toAdd);
            SelectionKey key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);
            serverCount++;
            serverChannels.add(channelToAdd);

        }
        // initializing turns array and counter to -1
        turns = new ArrayList<>(serverCount);

        // serverCount +1 because we need n+1 index because the ids start from 1
        counter = new ArrayList<Integer>(serverCount + 1);
        // first position is not valid (or we can count user inputs)
        counter.add(-1);
        for (int i = 0; i < serverCount; i++) {
            turns.add(true);
            counter.add(0);

        }
        counter.add(0);

    }

    /*
     * Servicing method, starts one thread for servicing requests and one for
     * user input
     */
    public void run() {

        System.out.println("Waiting for connections...");

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

                    // Check what event is available and deal with it
                    if (key.isAcceptable()) {
                        // a connection was accepted by a ServerSocketChannel
                        accept(key);
                    } else if (key.isReadable()) {
                        System.out.println("IS READABLE");
                        // a channel is ready for reading
                        parseInput(key);
                        /*
						 * } else if (key.isWritable()) { // a channel is ready
						 * for writing write(key);
						 */
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
                //TODO: fix
                //receivedMessage.fromString(msg);


                //FD increment
                int sendID = receivedMessage.getSenderId();
                incrementFD(sendID);

                System.out.println("Received query '"
                        + receivedMessage.getRequestType() + "' from node #"
                        + receivedMessage.getSenderId());
                Tag maxTag = findMaxTagFromSet(rep);

                switch (receivedMessage.getRequestType()) {
                    case "query":

                        sendMessage(channel,
                                new Message("query-ack", maxTag, rep.get(maxTag), id));
                        break;
                    case "pre-write":
                        Tag newTag = receivedMessage.getTag();
                        if (maxTag.compareTo(newTag) >= 0) {
                            System.out.println("Received tag smaller than local max tag");
                            break;
                        }

                        localTag = newTag;
                        localView = receivedMessage.getView();
                        localView.setStatus(View.Status.PRE);
                        rep.put(localTag, localView);
                        sendMessage(channel, new Message("pre-write-ack", localTag,
                                localView, id));
                        break;
                    case "finalize":
                        Tag bestTag = receivedMessage.getTag();

                        if (rep.containsKey(bestTag)) {
                            rep.get(bestTag).setStatus(View.Status.FIN);
                            sendMessage(channel, new Message("fin-ack", localTag,
                                    localView, getId()));
                        } else
                            rep.put(bestTag, new View(""));
                        localView.setStatus(View.Status.FIN);

                        break;
                    case "gossip":
                        Tag latestTag = receivedMessage.getTag();
                        if (rep.containsKey(latestTag))
                            rep.put(latestTag, receivedMessage.getView());
                        break;
                    case "userReadRequest":
                        read();
                        System.out.println("Replying to request with result: "
                                + localTag.toString() + " " + localView.toString());
                        sendMessage(channel,
                                new Message("success", localTag, localView, id));
                        /*try {
                            TimeUnit.SECONDS.sleep(30);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }*/
                        break;
                    case "userWriteRequest":
                        write(receivedMessage.getView());
                        System.out.println("Replying to request with result: success");
                        sendMessage(channel,
                                new Message("success", localTag, localView, id));
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

    private Tag findMaxTagFromSet(Map<Tag, View> rep) {

        Tag maxTag = new Tag(0, 0);

        Set<Tag> tags = rep.keySet();
        for (Tag tag : tags) {
            if (rep.get(tag).getStatus() == View.Status.PRE)
                continue;
            if (tag.compareTo(maxTag) > 0) {
                maxTag = tag;
            }

        }

        return maxTag;

    }

    private void sendMessage(SocketChannel s, Message m) throws IOException {

        writeBuffer.clear();
        writeBuffer.put(m.toString().getBytes()); // filling
        // buffer
        // with
        // message
        writeBuffer.flip();

        while (writeBuffer.hasRemaining())
            s.write(writeBuffer); // writing messsage to server
        writeBuffer.clear();

    }

    // if an accept request arises from selector
    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
                .channel();
        SocketChannel socketChannel = serverSocketChannel.accept();

        // setting non blocking option (to be 100% sure)
        socketChannel.configureBlocking(false);

        // other possible options
        // socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        // socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        // this because the selector has been accepted and we are waiting for a
        // read (otherwise the key.isReadable check won't work)
        socketChannel.register(selector, SelectionKey.OP_READ);

        System.out.println("Client" + socketChannel.getRemoteAddress()
                + " is connected");
    }

    // if a read request arises from selector
    private void read() {

        Tag maxTag;
        if ((maxTag = comm.query()) == null)
            return;
        View rcvView = comm.finalizeRead();
        localTag = maxTag;
        localView = rcvView;
        for (SocketChannel sc : serverChannels) {
            try {
                sendMessage(sc, new Message("gossip", maxTag, rcvView, getId()));
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
        Tag newTag = generateNewTag(maxTag);
        comm.preWrite(newTag, newView);
        localTag = newTag;
        localView = newView;
        rep.put(localTag, localView);
        if (comm.finalizeWrite())
            System.out.println("WE DID IT REDDIT");
        else
            System.out.println("WE LOST");

    }

    private Tag generateNewTag(Tag lastTag) {
        lastTag.setId(id);
        lastTag.setLabel(lastTag.getLabel() + 1);
        return lastTag;
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

        // Register an interest in writing on this channel because I just
        // connected to it
        //key.interestOps(SelectionKey.OP_WRITE);
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

    /* add last message received in the first position of the linked list */
    private void addMessage(Message msgToAdd) {

        if (messageList.size() >= LIST_SIZE)
            messageList.removeLast();

        messageList.add(msgToAdd);

    }

    public static Tag findMaxTagFromMessages(Message[] values) {
        Tag maxTag = new Tag(0, 0);
        for (Message msg : values) {
            if (msg != null) {
                if (msg.getTag().compareTo(maxTag) > 0) {
                    maxTag = msg.getTag();
                }
            }
        }

        return maxTag;
    }

    private NodeInfo getAddressFromString(String line, int id) {
        line = line.replace("/", "");
        line = line.replace(":", " ");
        String[] splitted = line.split(" ");
        return new NodeInfo(id, new InetSocketAddress(splitted[0],
                Integer.parseInt(splitted[1])));
    }

    public void removeCrashedServer(int i) {
        try {
            serverChannels.get(i).close();
            serverChannels.remove(i);
            serverCount--;
            turns.remove(i);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void incrementFD (int sendID){

        //resetting counter for the sender
        counter.set(sendID,0);

        for (int l = 1; l < counter.size(); l++) {

            // increment all the counters of the other servers if sender id is not the cycled id
            if (sendID == 0 || sendID == id)
                break;
            if (sendID != l)

                counter.set(l, (counter.get(l)) + 1); // set the cycled id as itself +1

        }

    }

    /* Getters and Setters */
    public LinkedList<Message> getMessageList() {
        return messageList;
    }

    public void setMessageList(LinkedList<Message> messageList) {
        this.messageList = messageList;
    }

    public ArrayList<SocketChannel> getServerChannels() {
        return serverChannels;
    }

    public void setServerChannels(ArrayList<SocketChannel> serverChannels) {
        this.serverChannels = serverChannels;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Map<Tag, View> getRep() {
        return rep;
    }

    public void setRep(Map<Tag, View> rep) {
        this.rep = rep;
    }

    public ArrayList<Boolean> getTurns() {
        return turns;
    }

    public void setTurns(ArrayList<Boolean> turns) {
        this.turns = turns;
    }

    public int getServerCount() {
        return serverCount;
    }

    public void setServerCount(int serverCount) {
        this.serverCount = serverCount;
    }

    public View getLocalView() {
        return localView;
    }

    public void setLocalView(View localView) {
        this.localView = localView;
    }

    public Tag getLocalTag() {
        return localTag;
    }

    public void setLocalTag(Tag localTag) {
        this.localTag = localTag;
    }

    public ArrayList<Integer> getCounter() {
        return counter;
    }

    public void setCounter(ArrayList<Integer> counter) {
        this.counter = counter;
    }

    public View getProposedView() {
        return proposedView;
    }

    public ArrayList<Integer> getProposedViewArray() {
        return proposedView.getIdArray();
    }

    public void setProposedView(View proposedView) {
        this.proposedView = proposedView;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void setIsMaster(boolean isMaster) {
        this.isMaster = isMaster;
    }

}
