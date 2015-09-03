package com.robustMRMW;

import NetworkPrimitives.Settings;

public class Main {

    public static void main(String[] args) {

        //TODO: should generate a new node with correct settings and that can correctly connect to every other node

        boolean flag = true;
        while(flag){

            if (args.length != 3){

                System.out.println("Usage: program <type> (1:server, 2:reader/writer) <id> <port>");
                flag = false;

            } else {

                //TODO: checks
                if (Integer.parseInt(args[0]) == 1) {

                    System.out.println("Starting server");
                    NetworkPrimitives.Settings init = new Settings(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
                    Node n = new Node(init);
                    n.setup();
                    //TODO: before running we have to be sure that a quorum is possible
                    n.run();
                } else {

                    System.out.println("Starting reader / writer");
                    UserInputProcess ui = new UserInputProcess();
                    ui.run();


                }

            }
        }

        System.out.println("Exiting...");



    }
}
