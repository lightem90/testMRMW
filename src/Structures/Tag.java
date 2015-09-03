package Structures;

import java.util.LinkedList;

/* Class used to identify an operation in the system, in this way we can implement a multi reader multi writer, take a look in the paper for a deeper explanation */
public class Tag implements Comparable<Tag>{

	//we can't send an infinite counter list so we limit the size of it
	private static int MAXIMUM_LIST_SIZE = 10;

	private int id;
	private int label; //not really correct
	private LinkedList<Counter> counters;


	//init a new Tag with an empty counter
	public Tag(int id, int label, int firstCounterValue) {

		this.id = id;
		this.label = label;
		this.counters = new LinkedList<>();
		//initializing with un-valid counter to avoid null pointer exc.
		counters.add(new Counter(id,firstCounterValue));


	}


	//compares two tag returning the highest one
	public Tag greatestTag(Tag t1, Tag t2) {

		//checking order: label -> counter -> id
		if (t1.label > t2.label)
			return t1;

		if (t1.label == t2.label) {
			Counter c1 = t1.getCounters().getFirst();
			Counter c2 = t2.getCounters().getFirst();

			//checking counter
			if (c1.getCounter() > c2.getCounter())
				return t1;

			if (c1.getCounter() == c2.getCounter())
				//returns t1 or t2 basing on id
				return c1.getId() > c2.getId() ? t1 : t2;

		}
		
		//means t2 > t1 because all other cases are handled
		return t2;

	}

	//TODO: How are we 100% SURE that the first is ALWAYS the bigger one?
	//just adding last (bigger) counter to the list
	public void addCounter(Counter c){

		if (counters.size() >= MAXIMUM_LIST_SIZE)
			counters.removeLast();

		counters.addFirst(c);
	}

	//we use only 2^32 values of counter (is 2^64), so we have to check if the label is exhausted or not
	public boolean isExhausted(){
		if (this.getCounters().getFirst().getCounter()+1 >= Integer.MAX_VALUE)
			return true;
		else return  false;
	}




	//comparing two tags means comparing: labels -> firstCounterElement -> ids (breaking simmetry)
	@Override
	public int compareTo(Tag anotherTag) {
		if(label == anotherTag.getLabel()) {

			//labels equal so I proceed comparing counter
			Counter t = anotherTag.getCounters().getFirst();
			if(t.getCounter()==counters.getFirst().getCounter())
				//counters equal: breaking simmetry
				return Integer.compare(counters.getFirst().getId(),t.getId());

			//counters are different
			else return Long.compare(counters.getFirst().getCounter(),t.getCounter());


		}
		//labels are different
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
