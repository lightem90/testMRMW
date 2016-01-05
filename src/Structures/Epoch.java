package Structures;

/**
 * Created by Matteo on 28/12/2015.
 */
public class Epoch implements Comparable<Epoch>{

    private long epoch;
    private int id;

    public Epoch(){}

    public Epoch(int id, int epoch)
    {
        this.id = id;
        this.epoch = epoch;

    }


    /* In our case the epoch will be simply incremented by one */
    public void incrementEpoch()
    {
        epoch++;
    }

    @Override
    public int compareTo(Epoch anotherEpoch)
    {
        if (epoch == anotherEpoch.getEpoch())
            return Integer.compare(id,anotherEpoch.getId());
        else
            return Long.compare(epoch,anotherEpoch.getEpoch());
    }
    @Override
    public int hashCode() {
        return 0;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return (this.compareTo((Epoch)o)==0);
    }

    /* Getters and Setters */
    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
