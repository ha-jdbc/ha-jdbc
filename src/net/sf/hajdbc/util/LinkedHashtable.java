/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hashtable implementation that tracks entry insertion order.
 * <br/>
 * <em>The following methods:
 * <ul>
 * 	<li>{@link #entrySet()}</li>
 * 	<li>{@link #keys()}</li>
 * 	<li>{@link #values()}</li>
 * </ul>
 * return unmodifiable collections - contrary to the behavior defined by java.util.Map interface.</em>
 * @author Paul Ferraro
 * @since 1.1.1
 */
public class LinkedHashtable<K, V> extends Hashtable<K, V>
{
	private static final long serialVersionUID = -8329636829651830808L;
	
	private List<K> keyList = new LinkedList<K>();
	
	/**
	 * 
	 */
	public LinkedHashtable()
	{
		super();
	}

	/**
	 * @param initialCapacity
	 * @param loadFactor
	 */
	public LinkedHashtable(int initialCapacity, float loadFactor)
	{
		super(initialCapacity, loadFactor);
	}

	/**
	 * @param initialCapacity
	 */
	public LinkedHashtable(int initialCapacity)
	{
		super(initialCapacity);
	}

	/**
	 * @param t
	 */
	public LinkedHashtable(Map<? extends K, ? extends V> map)
	{
		super(map.size());
		
		this.putAll(map);
	}

	/**
	 * @see java.util.Hashtable#clear()
	 */
	@Override
	public synchronized void clear()
	{
		super.clear();
		
		this.keyList.clear();
	}

	/**
	 * @see java.util.Hashtable#elements()
	 */
	@Override
	public synchronized Enumeration<V> elements()
	{
		final Iterator<K> keys = this.keyList.iterator();
		
		return new Enumeration<V>()
		{
			public boolean hasMoreElements()
			{
				return keys.hasNext();
			}

			public V nextElement()
			{
				return LinkedHashtable.this.get(keys.next());
			}
		};
	}

	/**
	 * @see java.util.Hashtable#entrySet()
	 */
	@Override
	public Set<Map.Entry<K, V>> entrySet()
	{
		LinkedHashSet<Map.Entry<K, V>> set = new LinkedHashSet<Map.Entry<K, V>>();
		
		for (K key: this.keyList)
		{
			for (Map.Entry<K, V> mapEntry: super.entrySet())
			{
				if (mapEntry.getKey().equals(key))
				{
					set.add(mapEntry);
					
					break;
				}
			}
		}
		
		return Collections.unmodifiableSet(set);
	}

	/**
	 * @see java.util.Hashtable#keys()
	 */
	@Override
	public synchronized Enumeration<K> keys()
	{
		return Collections.enumeration(this.keyList);
	}

	/**
	 * @see java.util.Hashtable#keySet()
	 */
	@Override
	public Set<K> keySet()
	{
		return Collections.unmodifiableSet(new LinkedHashSet<K>(this.keyList));
	}

	/**
	 * @see java.util.Hashtable#put(K, V)
	 */
	@Override
	public synchronized V put(K key, V value)
	{
		V old = super.put(key, value);
		
		if (old == null)
		{
			this.keyList.add(key);
		}
		
		return old;
	}

	/**
	 * @see java.util.Hashtable#putAll(java.util.Map)
	 */
	@Override
	public synchronized void putAll(Map< ? extends K, ? extends V> map)
	{
		for (Map.Entry<? extends K, ? extends V> entry: map.entrySet())
		{
			this.put(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * @see java.util.Hashtable#remove(java.lang.Object)
	 */
	@Override
	public synchronized V remove(Object key)
	{
		V value = super.remove(key);
		
		if (value != null)
		{
			this.keyList.remove(key);
		}
		
		return value;
	}

	/**
	 * @see java.util.Hashtable#values()
	 */
	@Override
	public Collection<V> values()
	{
		List<V> list = new ArrayList<V>(this.size());
		
		for (K key: this.keyList)
		{
			list.add(this.get(key));
		}
		
		return Collections.unmodifiableCollection(list);
	}
	
	/**
	 * @see java.util.Hashtable#clone()
	 */
	@Override
	public synchronized Object clone()
	{
		return new LinkedHashtable<K, V>(this);
	}
}
