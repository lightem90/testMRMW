package Structures;


import com.robustMRMW.Node;

/**
 * Created by Matteo on 19/06/2015.
 */
public class Counter {


    /* Two simple attributes to identify a write operation in the system, counter attribute can store 2^64 -1 values */
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



    //returns true if counter is greater than 2 to the power of 32
    boolean isExhausted (long c){

        if(c >= Integer.MAX_VALUE || c < Integer.MIN_VALUE) return true;
        else return false;
    }






}
