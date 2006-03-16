/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Paul Ferraro
 *
 */
public class BidirectionalHashMap<K, V> extends HashMap<K, V> implements BidirectionalMap<K, V>
{
	private static final long serialVersionUID = 7515823468929059569L;
	
	private HashMap<V, K> reverseMap;
	
	/**
	 * @param arg0
	 * @param arg1
	 */
	public BidirectionalHashMap(int initialCapacity, float loadFactor)
	{
		super(initialCapacity, loadFactor);

		this.reverseMap = new HashMap<V, K>(initialCapacity, loadFactor);
	}

	/**
	 * @param arg0
	 */
	public BidirectionalHashMap(int initialCapacity)
	{
		super(initialCapacity);

		this.reverseMap = new HashMap<V, K>(initialCapacity);
	}

	/**
	 * 
	 */
	public BidirectionalHashMap()
	{
		super();
		
		this.reverseMap = new HashMap<V, K>();
	}

	/**
	 * @param arg0
	 */
	public BidirectionalHashMap(Map<K, V> map)
	{
		super(map);
	}

	/**
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear()
	{
		super.clear();

		this.reverseMap.clear();
	}

	/**
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Object clone()
	{
		return new BidirectionalHashMap(this);
	}

	/**
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value)
	{
		return this.reverseMap.containsKey(value);
	}

	/**
	 * @see net.sf.hajdbc.util.BidirectionalMap#getKey(java.lang.Object)
	 */
	public K getKey(Object value)
	{
		return this.reverseMap.get(value);
	}

	/**
	 * @see java.util.Map#put(K, V)
	 */
	@Override
	public V put(K key, V value)
	{
		this.reverseMap.put(value, key);
		
		return super.put(key, value);
	}

	/**
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> map)
	{
		super.putAll(map);
		
		for (Map.Entry<? extends K, ? extends V> mapEntry: map.entrySet())
		{
			this.reverseMap.put(mapEntry.getValue(), mapEntry.getKey());
		}
	}

	/**
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public V remove(Object key)
	{
		V value = super.remove(key);
		
		this.reverseMap.remove(value);
		
		return value;
	}

	/**
	 * @see net.sf.hajdbc.util.BidirectionalMap#removeValue(java.lang.Object)
	 */
	public K removeValue(Object value)
	{
		K key = this.reverseMap.remove(value);
		
		super.remove(key);
		
		return key;
	}

	/**
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<V> values()
	{
		return this.reverseMap.keySet();
	}
}
