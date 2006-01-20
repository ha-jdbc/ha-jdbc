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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public final class Collections
{
	/**
	 * Casts the specified map into a typed map using the specified type.
	 * @param <K> target type for map keys
	 * @param <V> target type for map values
	 * @param map
	 * @param targetKeyClass 
	 * @param targetValueClass 
	 * @return a Map parameterized by the specified key anv value types
	 */
	public static <K, V> Map<K, V> cast(Map map, Class<K> targetKeyClass, Class<V> targetValueClass)
	{
		return new TypedMap(map, targetKeyClass, targetValueClass);
	}
	
	/**
	 * Casts the specified collection into a typed collection using the specified type.
	 * @param <T> target type for collection elements
	 * @param collection
	 * @param targetClass
	 * @return a Collection parameterized by the specified type
	 */
	public static <T> Collection<T> cast(Collection collection, Class<T> targetClass)
	{
		return new TypedCollection(collection, targetClass);
	}
	
	/**
	 * Casts the specified set into a typed set using the specified type.
	 * @param <T> target type for set elements
	 * @param set
	 * @param targetClass
	 * @return a Set parameterized by the specified type
	 */
	public static <T> Set<T> cast(Set set, Class<T> targetClass)
	{
		return new TypedSet(set, targetClass);
	}
	
	/**
	 * Casts the specified list into a typed list using the specified type.
	 * @param <T> target type for list elements
	 * @param list
	 * @param targetClass
	 * @return a List parameterized by the specified type
	 */
	public static <T> List<T> cast(List list, Class<T> targetClass)
	{
		return null;
	}
	
	private Collections()
	{
		// Hidden
	}
	
	private static class TypedIterator<E> implements Iterator<E>
	{
		private Iterator iterator;
		protected Class<E> targetClass;
		
		/**
		 * Constructs a new TypedIterator.
		 * @param iterator
		 * @param targetClass
		 */
		public TypedIterator(Iterator iterator, Class<E> targetClass)
		{
			this.iterator = iterator;
			this.targetClass = targetClass;
		}

		/**
		 * @see java.util.Iterator#hasNext()
		 */
		public boolean hasNext()
		{
			return this.iterator.hasNext();
		}

		/**
		 * @see java.util.Iterator#next()
		 */
		public E next()
		{
			return this.targetClass.cast(this.iterator.next());
		}

		/**
		 * @see java.util.Iterator#remove()
		 */
		public void remove()
		{
			this.iterator.remove();
		}
	}
	
	private static class TypedCollection<E> implements Collection<E>
	{
		private Collection<Object> collection;
		protected Class<E> targetClass;
		
		/**
		 * Constructs a new TypedCollection.
		 * @param collection
		 * @param targetClass
		 */
		public TypedCollection(Collection collection, Class<E> targetClass)
		{
			this.collection = collection;
			this.targetClass = targetClass;
		}
		
		/**
		 * @see java.util.Collection#add(E)
		 */
		public boolean add(E element)
		{
			return this.collection.add(element);
		}

		/**
		 * @see java.util.Collection#addAll(java.util.Collection)
		 */
		public boolean addAll(Collection<? extends E> c)
		{
			return this.collection.addAll(c);
		}

		/**
		 * @see java.util.Collection#clear()
		 */
		public void clear()
		{
			this.collection.clear();
		}

		/**
		 * @see java.util.Collection#contains(java.lang.Object)
		 */
		public boolean contains(Object element)
		{
			return this.collection.contains(element);
		}

		/**
		 * @see java.util.Collection#containsAll(java.util.Collection)
		 */
		public boolean containsAll(Collection<?> c)
		{
			return this.collection.containsAll(c);
		}

		/**
		 * @see java.util.Collection#isEmpty()
		 */
		public boolean isEmpty()
		{
			return this.collection.isEmpty();
		}

		/**
		 * @see java.util.Collection#iterator()
		 */
		public Iterator<E> iterator()
		{
			return new TypedIterator(this.collection.iterator(), this.targetClass);
		}

		/**
		 * @see java.util.Collection#remove(java.lang.Object)
		 */
		public boolean remove(Object element)
		{
			return this.collection.remove(element);
		}

		/**
		 * @see java.util.Collection#removeAll(java.util.Collection)
		 */
		public boolean removeAll(Collection<?> c)
		{
			return this.collection.removeAll(c);
		}

		/**
		 * @see java.util.Collection#retainAll(java.util.Collection)
		 */
		public boolean retainAll(Collection<?> c)
		{
			return this.collection.retainAll(c);
		}

		/**
		 * @see java.util.Collection#size()
		 */
		public int size()
		{
			return this.collection.size();
		}

		/**
		 * @see java.util.Collection#toArray()
		 */
		public Object[] toArray()
		{
			return this.collection.toArray();
		}

		/**
		 * @see java.util.Collection#toArray(T[])
		 */
		public <T> T[] toArray(T[] array)
		{
			return this.collection.toArray(array);
		}
	}

	private static class TypedSet<E> extends TypedCollection<E> implements Set<E>
	{
		/**
		 * Constructs a new TypedSet.
		 * @param collection
		 * @param targetClass
		 */
		public TypedSet(Collection collection, Class<E> targetClass)
		{
			super(collection, targetClass);
		}
	}

	private static class TypedList<E> extends TypedCollection<E> implements List<E>
	{
		private List list;
		
		/**
		 * Constructs a new TypedList.
		 * @param list
		 * @param targetClass
		 */
		public TypedList(List<Object> list, Class<E> targetClass)
		{
			super(list, targetClass);
			
			this.list = list;
		}
		
		/**
		 * @see java.util.List#addAll(int, java.util.Collection)
		 */
		public boolean addAll(int index, Collection<? extends E> collection)
		{
			return this.list.addAll(index, collection);
		}

		/**
		 * @see java.util.List#get(int)
		 */
		public E get(int index)
		{
			return this.targetClass.cast(this.list.get(index));
		}

		/**
		 * @see java.util.List#set(int, E)
		 */
		public E set(int index, E value)
		{
			return this.targetClass.cast(this.list.set(index, value));
		}

		/**
		 * @see java.util.List#add(int, E)
		 */
		public void add(int index, E value)
		{
			this.list.add(index, value);
		}

		/**
		 * @see java.util.List#remove(int)
		 */
		public E remove(int index)
		{
			return this.targetClass.cast(this.list.remove(index));
		}

		/**
		 * @see java.util.List#indexOf(java.lang.Object)
		 */
		public int indexOf(Object element)
		{
			return this.list.indexOf(element);
		}

		/**
		 * @see java.util.List#lastIndexOf(java.lang.Object)
		 */
		public int lastIndexOf(Object element)
		{
			return this.lastIndexOf(element);
		}

		/**
		 * @see java.util.List#listIterator()
		 */
		public ListIterator<E> listIterator()
		{
			return new TypedListIterator(this.list.listIterator());
		}

		/**
		 * @see java.util.List#listIterator(int)
		 */
		public ListIterator<E> listIterator(int index)
		{
			return new TypedListIterator(this.list.listIterator(index));
		}

		/**
		 * @see java.util.List#subList(int, int)
		 */
		public List<E> subList(int startIndex, int endIndex)
		{
			return new TypedList(this.list.subList(startIndex, endIndex), this.targetClass);
		}
		
		private class TypedListIterator extends TypedIterator<E> implements ListIterator<E>
		{
			private ListIterator iterator;
			
			/**
			 * Constructs a new TypedListIterator.
			 * @param iterator
			 */
			public TypedListIterator(ListIterator iterator)
			{
				super(iterator, TypedList.this.targetClass);
				
				this.iterator = iterator;
			}

			/**
			 * @see java.util.ListIterator#hasPrevious()
			 */
			public boolean hasPrevious()
			{
				return this.iterator.hasPrevious();
			}

			/**
			 * @see java.util.ListIterator#previous()
			 */
			public E previous()
			{
				return this.targetClass.cast(this.iterator.previous());
			}

			/**
			 * @see java.util.ListIterator#nextIndex()
			 */
			public int nextIndex()
			{
				return this.iterator.nextIndex();
			}

			/**
			 * @see java.util.ListIterator#previousIndex()
			 */
			public int previousIndex()
			{
				return this.iterator.previousIndex();
			}

			/**
			 * @see java.util.ListIterator#set(E)
			 */
			public void set(E element)
			{
				this.iterator.set(element);
			}

			/**
			 * @see java.util.ListIterator#add(E)
			 */
			public void add(E element)
			{
				this.iterator.add(element);
			}
		}
	}
	
	private static class TypedMap<K, V> implements Map<K, V>
	{
		private Map map;
		private Class<K> targetKeyClass;
		private Class<V> targetValueClass;
		
		/**
		 * Constructs a new TypedMap.
		 * @param map
		 * @param targetKeyClass 
		 * @param targetValueClass 
		 */
		public TypedMap(Map map, Class<K> targetKeyClass, Class<V> targetValueClass)
		{
			this.map = map;
			this.targetKeyClass = targetKeyClass;
			this.targetValueClass = targetValueClass;
		}
		
		/**
		 * @see java.util.Map#size()
		 */
		public int size()
		{
			return this.map.size();
		}

		/**
		 * @see java.util.Map#isEmpty()
		 */
		public boolean isEmpty()
		{
			return this.map.isEmpty();
		}

		/**
		 * @see java.util.Map#containsKey(java.lang.Object)
		 */
		public boolean containsKey(Object key)
		{
			return this.map.containsKey(key);
		}

		/**
		 * @see java.util.Map#containsValue(java.lang.Object)
		 */
		public boolean containsValue(Object value)
		{
			return this.map.containsValue(value);
		}

		/**
		 * @see java.util.Map#get(java.lang.Object)
		 */
		public V get(Object key)
		{
			return this.targetValueClass.cast(this.map.get(key));
		}

		/**
		 * @see java.util.Map#put(K, V)
		 */
		public V put(K key, V value)
		{
			return this.targetValueClass.cast(this.map.put(key, value));
		}

		/**
		 * @see java.util.Map#remove(java.lang.Object)
		 */
		public V remove(Object key)
		{
			return this.targetValueClass.cast(this.map.remove(key));
		}

		/**
		 * @see java.util.Map#putAll(java.util.Map)
		 */
		public void putAll(Map<? extends K, ? extends V> m)
		{
			this.map.putAll(m);
		}

		/**
		 * @see java.util.Map#clear()
		 */
		public void clear()
		{
			this.map.clear();
		}

		/**
		 * @see java.util.Map#keySet()
		 */
		public Set<K> keySet()
		{
			return Collections.cast(this.map.keySet(), this.targetKeyClass);
		}
		
		/**
		 * @see java.util.Map#values()
		 */
		public Collection<V> values()
		{
			return Collections.cast(this.map.values(), this.targetValueClass);
		}
		
		/**
		 * @see java.util.Map#entrySet()
		 */
		public Set<Map.Entry<K, V>> entrySet()
		{
			return this.map.entrySet();
		}
	}
}
