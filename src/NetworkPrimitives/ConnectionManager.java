package NetworkPrimitives;

import EncoderDecoder.EncDec;
import Structures.Message;
import Structures.Tag;
import Structures.View;
import com.robustMRMW.FailureDetector;
import com.robustMRMW.Node;

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

/**
 * Created by Matteo on 22/07/2015.
 */
public class ConnectionManager {

    //Class constants
    private final static String ADDRESS_PATH = "address.txt";
    private final static String ADDRESS = "localhost";
    private final static int PORT = 0;
    private final static int LIST_SIZE = 10;


    // Class buffer
    public static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    //TODO: this class should implement all the methods for setting up the connection with the other server and should implement the server select routine


    // Class connection variables
    private int serverCount = 0;
    private SocketAddress hostAddress;
    private Selector selector;
    private ArrayList<SocketChannel> serverChannels;
    private ArrayList<InetSocketAddress> otherNodesAddress;
    private Map<Tag, View> rep;

    private LinkedList<Message> messageList;

    // Custom objects
    private FailureDetector FD;
    private Communicate comm;
    private Node n; //keeping this for now
    private EncDec ED;



    public ConnectionManager(Node c){


        System.out.println("Setting up node with id: " + c.getMySett().getNodeId() + " and port: " + c.getMySett().getPort());
        hostAddress = new InetSocketAddress(ADDRESS,c.getMySett().getPort());
        rep = new HashMap<>();
        ED = new EncDec();
        n = c;

        //initializing rep with first tag and current view (only me active)
        rep.put(new Tag(c.getMySett().getNodeId(),0),new View(String.valueOf(c.getMySett().getNodeId())));


    }

    /* initialize and bind selector to current address / port */
    public void init() {

        otherNodesAddress = new ArrayList<InetSocketAddress>();
        serverChannels = new ArrayList<>();

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

            // writing local address to file
            Path filePath = Paths.get(ADDRESS_PATH);
            String toWrite = String.format(serverChannel.getLocalAddress()
                    + "%n");
            System.out.println("The address of this node is: " + toWrite);

            // writing the address of the new node on file so every other  node can connect to it
            // TODO: should not write to file cause we create it manually or in the script?
            Files.write(filePath, toWrite.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Press enter when all server wrote the address");
        Scanner s = new Scanner(System.in);
        s.nextLine();

        System.out.println("Reading addresses from file " + ADDRESS_PATH);


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
                    otherNodesAddress.add(getAddressFromString(line));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* Connecting to all other nodes */
    public void connect() throws IOException {

        System.out.println(n.getMySett().getNodeId() + " Connecting to existing nodes");

        for (InetSocketAddress address : otherNodesAddress) {

            SocketChannel channelToAdd = SocketChannel.open();
            channelToAdd.configureBlocking(false);

            SocketAddress toAdd = address;

            // connect to server with inetadresses previously read, TODO: we assume connect works
            channelToAdd.connect(toAdd);
            SelectionKey key = channelToAdd.register(selector, SelectionKey.OP_CONNECT);
            serverCount++;
            serverChannels.add(channelToAdd);

        }


        //initializing failure detector (now that I know the number of nodes)

        //FD = new FailureDetector(serverCount);

        //initializing communicate
        comm = new Communicate(n,this);

    }


    /* Servicing method, starts one thread for servicing requests and one for user input  */
    public void run() {

        System.out.println("Waiting for connections...");

        messageList = new LinkedList<Message>();

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
						 * } else if (key.isWritable()) { // a channel is ready for writing write(key);
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

                receivedMessage = ED.decode(msg);
                addMessage(receivedMessage);

                //FD increment
                int sendID = receivedMessage.getSenderId();
                //TODO: commented for now, we have to decide how to handle ids
                //FD.updateFDForNode(sendID);

                System.out.println("Received query '"
                        + receivedMessage.getRequestType() + "' from node #"
                        + receivedMessage.getSenderId());
                Tag maxTag = findMaxTagFromSet(rep);

                switch (receivedMessage.getRequestType()) {
                    case "query":

                        sendMessage(channel,
                                new Message("query-ack", maxTag, rep.get(maxTag), n.getMySett().getNodeId()));
                        break;
                    case "pre-write":
                        Tag newTag = receivedMessage.getTag();
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
                        Tag bestTag = receivedMessage.getTag();

                        if (rep.containsKey(bestTag)) {
                            rep.get(bestTag).setStatus(View.Status.FIN);
                            sendMessage(channel, new Message("fin-ack", n.getLocalTag(),
                                    n.getLocalView(), n.getMySett().getNodeId()));
                        } else
                            rep.put(bestTag, new View(""));
                        n.getLocalView().setStatus(View.Status.FIN);

                        break;
                    case "gossip":
                        Tag latestTag = receivedMessage.getTag();
                        if (rep.containsKey(latestTag))
                            rep.put(latestTag, receivedMessage.getView());
                        break;
                    case "userReadRequest":
                        read();
                        System.out.println("Replying to request with result: "
                                + n.getLocalTag().toString() + " " + n.getLocalView().toString());
                        sendMessage(channel,
                                new Message("success", n.getLocalTag(),
                                        n.getLocalView(), n.getMySett().getNodeId()));
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

        // setting non blocking option (to be 100% sure)
        socketChannel.configureBlocking(false);

        // other possible options
        // socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        // socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        // this because the selector has been accepted and we are waiting for a
        // read (otherwise the key.isReadable check won't work)
        socketChannel.register(selector, SelectionKey.OP_READ);

        System.out.println("Client listening on local port: " + socketChannel.getLocalAddress());
    }

    // if a read request arises from selector
    private void read() {

        Tag maxTag;
        if ((maxTag = comm.query()) == null)
            return;
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
        Tag newTag = generateNewTag(maxTag);
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




    //Utilities

    private InetSocketAddress getAddressFromString(String line) {
        line = line.replace("/", "");
        line = line.replace(":", " ");
        String[] splitted = line.split(" ");
        return new InetSocketAddress(splitted[0],
                Integer.parseInt(splitted[1]));
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

    private Tag generateNewTag(Tag lastTag) {
        lastTag.setId(n.getMySett().getNodeId());
        lastTag.setLabel(lastTag.getLabel() + 1);
        return lastTag;
    }


    /* add last message received in the first position of the linked list */
    private void addMessage(Message msgToAdd) {

        if (messageList.size() >= LIST_SIZE)
            messageList.removeLast();

        messageList.add(msgToAdd);

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

    public FailureDetector getFD() {
        return FD;
    }

    public void setFD(FailureDetector FD) {
        this.FD = FD;
    }



}
