package Structures;

import com.robustMRMW.Node;

import java.util.LinkedList;

public class Tag implements Comparable<Tag>{

	private int id;
	private int label;
	private LinkedList<Counter> counters;


	//intializes a new Tag with an empty counter
	public Tag(int id, int label) {

		this.id = id;
		this.label = label;
		this.counters = new LinkedList<Counter>();
		//initializing with un-valid counter to avoid null pointer exc.
		counters.add(new Counter(-1,-1));


	}

	//TODO: check counter as well
	public Tag greatestTag(Tag t1, Tag t2) {

		if (t1.label > t2.label)
			return t1;

		//if labels are equal I check id (they cannot be equal)
		if (t1.label == t2.label) {
			Counter c1 = t1.getCounters().getFirst();
			Counter c2 = t2.getCounters().getFirst();
			if (c1.getCounter() > c2.getCounter())
				return t1;
			if (c1.getCounter() == c2.getCounter())
				return c1.getId() > c2.getId() ? t1 : t2;

		}
		
		//t2 is for sure > t1
		return t2;

	}



	@Override
	public int compareTo(Tag anotherTag) {
		if(this.label == anotherTag.getLabel())
			return Integer.compare(id,anotherTag.getId());
		return Integer.compare(label,anotherTag.getLabel());
	}
	@Override
	public int hashCode() {
		return 0;
	}
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return (this.compareTo((Tag)o)==0);
	}


	/* Getters and Setters */
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getLabel() {
		return label;
	}

	public void setLabel(int label) {
		this.label = label;
	}

	public LinkedList<Counter> getCounters() {return counters;}

	public void setCounters(LinkedList<Counter> counters) {this.counters = counters;}


}
