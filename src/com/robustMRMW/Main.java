package com.robustMRMW;

import NetworkPrimitives.Settings;

public class Main {

    public static void main(String[] args) {

        boolean flag = true;
        while(flag){

            if (args.length != 1){

                System.out.println("Usage: program <id>");
                flag = false;

            } else {

                System.out.println("Starting server with id: "+(args[0]));
                Settings init = new Settings(Integer.parseInt(args[0]));
                Node n = new Node(init);
                System.out.println("Node initialized........");
                n.setup();
                System.out.println("Node setup completed.........");
                n.run();

            }
        }

        System.out.println("Exiting...");



    }
}
