package com.gigaspaces.streaming.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.query.IdQuery;

/**
 * Simple queue implementation with a fixed size that throws away the
 * oldest members on overflow.  Intended for use via the change api for
 * statistics gathering
 *  
 * @author DeWayne
 *
 */
public class FixedNumberQueue<T extends Number> extends LinkedList<T>{
	private List<T> _delegate=new LinkedList<T>();
	private int limit;
	
	public FixedNumberQueue(){}
	
	public FixedNumberQueue(int size){
		this.limit=size;
	}

	public int size() {
		return _delegate.size();
	}

	public boolean isEmpty() {
		return _delegate.isEmpty();
	}

	public boolean contains(Object o) {
		return _delegate.contains(o);
	}

	public Iterator<T> iterator() {
		return _delegate.iterator();
	}

	public Object[] toArray() {
		return _delegate.toArray();
	}

	public <T> T[] toArray(T[] a) {
		return _delegate.toArray(a);
	}

	public boolean remove(Object o) {
		return _delegate.remove(o);
	}

	public boolean containsAll(Collection<?> c) {
		return _delegate.containsAll(c);
	}

	public boolean addAll(int index, Collection<? extends T> c) {
		return _delegate.addAll(index, c);
	}

	public boolean removeAll(Collection<?> c) {
		return _delegate.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		return _delegate.retainAll(c);
	}

	public void clear() {
		_delegate.clear();
	}

	public boolean equals(Object o) {
		return _delegate.equals(o);
	}

	public int hashCode() {
		return _delegate.hashCode();
	}

	public T get(int index) {
		return _delegate.get(index);
	}

	public T set(int index, T element) {
		return _delegate.set(index, element);
	}

	public void add(int index, T element) {
		_delegate.add(index, element);
	}

	public T remove(int index) {
		return _delegate.remove(index);
	}

	public int indexOf(Object o) {
		return _delegate.indexOf(o);
	}

	public int lastIndexOf(Object o) {
		return _delegate.lastIndexOf(o);
	}

	public ListIterator<T> listIterator() {
		return _delegate.listIterator();
	}

	public ListIterator<T> listIterator(int index) {
		return _delegate.listIterator(index);
	}

	public List<T> subList(int fromIndex, int toIndex) {
		return _delegate.subList(fromIndex, toIndex);
	}

	@Override
	public boolean add(T e) {
		System.out.println("adding "+e+" size="+this.size()+" limit="+limit);
		if(_delegate.add(e)){
			int excess=this.size()-limit;
			for(int i=0;i<excess;i++){
				System.out.println("add removing");
				_delegate.remove(0);
			}
			return true;
		}
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		System.out.println("addalling "+c+" size="+this.size()+" limit="+limit);
		if(_delegate.addAll(c)){
			int excess=this.size()-limit;
			for(int i=0;i<excess;i++){
				System.out.println("addall removing");
				_delegate.remove(0);
			}
			return true;
		}
		return false;
	}

	/**
	 * Gets the average of queue entry values.
	 * @return
	 */
	public double getAverage(){
		double sum=0;
		for(Iterator<T> it=this.iterator();it.hasNext();){
			sum+=it.next().doubleValue();
		}
		return sum/this.size();
	}
	
	public double getMax(){
		double max=0;
		for(Iterator<T> it=this.iterator();it.hasNext();){
			double val=it.next().doubleValue();
			if(max<val)max=val;
		}
		return max; 
	}
	
	public double getMin(){
		double min=0;
		for(Iterator<T> it=this.iterator();it.hasNext();){
			double val=it.next().doubleValue();
			if(min>val)min=val;
		}
		return min; 
	}
	
	@SpaceClass
	public static class Test{
		private Integer id=null;
		private FixedNumberQueue<Integer> q=new FixedNumberQueue<Integer>(3);
		
		public Test(){}

		public Test(int i) {
			this.id=i;
		}

		public FixedNumberQueue getQ() {
			return q;
		}

		public void setQ(FixedNumberQueue q) {
			this.q = q;
		}

		@SpaceId(autoGenerate=false)
		@SpaceRouting
		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}
	
	public static void main(String[] args){
		UrlSpaceConfigurer usc=new UrlSpaceConfigurer("/./space");
		try{
		GigaSpace space=new GigaSpaceConfigurer(usc.space()).gigaSpace();
		space.write(new Test(0));
		
		ChangeSet cs=new ChangeSet();
		cs.addToCollection("q", 1);
		cs.addToCollection("q", 2);
		cs.addToCollection("q", 3);
		cs.addToCollection("q", 4);
		space.change(new IdQuery<Test>(Test.class,0),cs);
		Test after=space.readById(Test.class,0);
		
		System.out.println(after.getQ());
		}
		finally{
			try {
				usc.destroy();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}

