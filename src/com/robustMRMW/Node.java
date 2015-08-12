package com.robustMRMW;

import NetworkPrimitives.Communicate;
import NetworkPrimitives.Settings;
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

    /* Constructor with custom port */
    public Node(Settings settings) {

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
