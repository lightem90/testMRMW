package NetworkPrimitives;

import Structures.Message;


public class Collector implements Runnable {

    Message req;
    Message[] returnValues;
    Communicate mC;


    public Collector(Message request, Message[] val, Communicate c) {

        System.out.println("Initializing collector");
        req = request;
        returnValues = val;
        mC = c;

    }


    @Override
    public void run() {

        System.out.println("Running collector");
        //calling wait for quorum of passed object.. doesn't work tho, all send trigger the exception and the message is not received
        returnValues = mC.waitForQuorum(req);
    }

    public Message getReq() {
        return req;
    }

    public Message[] getReturnValues() {
        return returnValues;
    }

    public Communicate getmC() {
        return mC;
    }
}
