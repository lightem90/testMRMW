package NetworkPrimitives;

import Structures.Message;
import Structures.Tag;
import Structures.View;
import com.robustMRMW.Node;

import java.io.IOException;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Matteo on 22/07/2015.
 */
public class ConnectionManager {

    //TODO: this class should implement all the methods for setting up the connection with the other server and should implement the server select routine


    private ArrayList<SocketChannel> serverChannels;

    // Class connection variables
    private final static String ADDRESS_PATH = "address.txt";
    private final static String ADDRESS = "localhost";
    private final static int PORT = 0;
    private int serverCount = 0;
    private SocketAddress hostAddress;
    private ArrayList<Node> otherNodesAddress;
    private Selector selector;

    // Class buffer
    public static final int BUFFER_SIZE = 1024; // random for now
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);


    public ConnectionManager(Settings settings){








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



}
