package com.robustMRMW;

import EncoderDecoder.EncDec;
import Structures.Message;
import Structures.Tag;
import Structures.View;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/* process to send read/write request to server */
public class UserInputProcess {

    private static final int BUFFER_SIZE = 1024;
    private ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(BUFFER_SIZE);

    public UserInputProcess() {
    }

    public void run() {

        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(System.in);
        boolean flag = true;
        String command;
        List<String> cmdList = new ArrayList<>();
        View inputView = new View("");

        while (flag) {

            Message usrMsg;
            Message rcvMsg;
            String rcvStr = "";
            System.out.println(String
                    .format("Commands:%n -connect address port%n -exit"));

            System.out.println("Remember: connecting will delete address file!");

            // getting user command
            command = scanner.nextLine();

            cmdList.clear();
            String[] tmp = command.split("\\s+");
            for (int i = 0; i < tmp.length; i++)
                cmdList.add(tmp[i]); // filling cmdList

            // handling user input
            String cmd = cmdList.get(0);
            String address = cmdList.get(1);
            int port = Integer.parseInt(cmdList.get(2));

            switch (cmd) {

                // the client is trying to connect
                case "connect":
                    System.out.println("Connecting to: " + cmdList.get(1) + " at "
                            + cmdList.get(2));

                    File f = new File("address.txt");
                    f.delete();

                    // initializing connection with inputed values (no checks)
                    SocketAddress serverAddress = new InetSocketAddress(address,
                            port);
                    SocketChannel channelToServer;
                    try {
                        channelToServer = SocketChannel.open();
                        channelToServer.configureBlocking(true);
                        channelToServer.connect(serverAddress);
                        //channelToServer.finishConnect();
                    } catch (IOException e) {
                        // connection is not possible, address and/or port may be
                        // wrong
                        System.out.println("Error in connecting");
                        e.printStackTrace();
                        break;
                    }

                    // if connection is succesfull I'll ask for read or write
                    // operations
                    boolean innerFlag = true;

                    while (innerFlag) {
                        System.out.println(String
                                .format("Enter operation: %n -read%n -write%n -exit"));

                        switch (scanner.nextLine()) {

                            // in this case I want to read the current view from a server
                            case "read":

                                // creating custom message
                                usrMsg = new Message("userReadRequest", new Tag(0, 0,-1), new View("userReadReq"), 0);

                                try {
                                    sendMessage(channelToServer, usrMsg);
                                } catch (IOException e) {
                                    System.out.println("Cannot write request to server");
                                    e.printStackTrace();
                                    break;
                                }

                                // reading reply from server
                                try {
                                    rcvMsg = receiveMessage(channelToServer);
                                } catch (IOException e) {
                                    System.out.println("Cannot read message from server");
                                    e.printStackTrace();
                                    break;
                                }

                                System.out.println("The current view with quorum is: " + rcvMsg.getView().toString() + " with tag " + rcvMsg.getTag().toString());
                                break;

                            // in this case I'm forcing a new view
                            case "write":
                                System.out.println("Enter new value for view");

                                View newView = new View(scanner.nextLine());
                                // creating custom message
                                usrMsg = new Message("userWriteRequest", new Tag(0, 0,-1),
                                        newView, 0);

                                try {
                                    sendMessage(channelToServer, usrMsg);
                                } catch (IOException e) {
                                    System.out.println("Cannot write request to server");
                                    e.printStackTrace();
                                    break;
                                }

                                // reading reply from server
                                try {
                                    rcvMsg = receiveMessage(channelToServer);
                                } catch (IOException e) {
                                    System.out.println("Cannot read message from server");
                                    e.printStackTrace();
                                    break;
                                }
                                if (rcvMsg.getRequestType().equals("success"))
                                    System.out.println("Write operation successful (new Tag: " + rcvMsg.getTag().toString() + ")");
                                break;


                            case "exit":
                                System.out.println("Exiting...");
                                innerFlag = false;
                                break;

                            // if the user didn't input read write or exit
                            default:
                                System.out.println("Wrong command.. Press enter to continue");
                                scanner.nextLine();
                                break;
                        }
                    }
                case "exit":
                    System.out.println("Exiting...");
                    flag = false;
                    break;

                default:
                    System.out.println("Wrong command.. Press enter to continue");
                    scanner.nextLine();
                    break;
            }
        }
    }

    private void sendMessage(SocketChannel s, Message m) throws IOException {

        EncDec ed = new EncDec();
        String toSend = ed.encode(m);

        writeBuffer.clear();
        writeBuffer.put(toSend.getBytes()); // filling buffer with message
        writeBuffer.flip();                         //always flip to set properly position and limit


        while (writeBuffer.hasRemaining())
            s.write(writeBuffer); // writing messsage to server
    }

    private Message receiveMessage(SocketChannel s) throws IOException {

        EncDec ed = new EncDec();
        String rcvStr = "";
        int count;
        readBuffer.clear();

        int flag = 0;
        while (flag == 0) {
            count = s.read(readBuffer);
            if (count > 0) {
                flag = 1;
               }

            // flip the buffer to start reading
            readBuffer.flip();
            rcvStr += Charset.defaultCharset().decode(readBuffer);
        }

        readBuffer.clear();
        // handling correctly received message
        Message msg = ed.decode(rcvStr);
        return msg;

    }

}
