package com.robustMRMW;

import NetworkPrimitives.Settings;

public class Main {

    public static void main(String[] args) {

        //TODO: should generate a new node with correct settings and that can correctly connect to each other node

        boolean flag = true;
        while(flag){

            if (args.length != 2){

                System.out.println("Usage: program -id -port");
                flag = false;

            } else {

                NetworkPrimitives.Settings init = new Settings(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
                Node n = new Node(init);
                n.setup();
                n.run();

            }
        }

        System.out.println("Exiting...");



    }
}
