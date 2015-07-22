package Structures;


import com.robustMRMW.Node;

/**
 * Created by Matteo on 19/06/2015.
 */
public class Counter {




    private int id;
    private long counter;

    public Counter (){}

    public Counter (int idS, long newC){

        id = idS;
        counter = newC;
    }


    /* Getters and Setters */
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public void setCounter(long counter) {
        this.counter = counter;
    }
    public long getCounter() {
        return counter;
    }

    //encodes counter to string using Node.SEPARATOR parameter
    public String counterToString(Counter c){

        return String.valueOf(id) + Node.SEPARATOR + String.valueOf(counter);
    }

    //decodes counter from string
    public void counterFromString(String s){

        String tokens[] = s.split(Node.SEPARATOR);

        if (tokens.length == 2) {
            this.id = Integer.parseInt(tokens[0]);
            this.counter = Long.parseLong(tokens[1]);
            }
        else System.out.print("Cannot get counter from string");
    }

    //returns true if counter is greater than 2 to the power of 32
    boolean isExhausted (long c){

        if(c >= Integer.MAX_VALUE || c < Integer.MIN_VALUE) return true;
        else return false;
    }






}
