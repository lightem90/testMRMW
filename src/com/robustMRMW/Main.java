package com.robustMRMW;

import NetworkPrimitives.Settings;

public class Main {

    public static void main(String[] args) {

        //TODO: should generate a new node with correct settings and that can correctly connect to every other node

        boolean flag = true;
        while(flag){

            if (args.length != 1){

                System.out.println("Usage: program <id>");
                flag = false;

            } else {

                //TODO: checks
                //if (Integer.parseInt(args[0]) == 1) {

                    System.out.println("Starting server with id: "+(args[0]));
                    NetworkPrimitives.Settings init = new Settings(Integer.parseInt(args[0]));
                    Node n = new Node(init);
                    n.setup();
                    n.run();
                /*} else {

                    System.out.println("Starting reader / writer");
                    UserInputProcess ui = new UserInputProcess();
                    ui.run();


                }
                */

            }
        }

        System.out.println("Exiting...");



    }
}
