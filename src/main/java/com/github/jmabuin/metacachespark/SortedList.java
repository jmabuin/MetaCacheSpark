package com.github.jmabuin.metacachespark;

import java.io.Serializable;
import java.util.Iterator;

//Interface
public interface SortedList<T>  {

    /**
     * Inserts an element to the list keeping the list sorted.
     */
    public void add(T element);
    public Iterator<T> iterator();
    public boolean isEmpty();
    public T get(int i);
    public int size();

}