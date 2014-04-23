/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.util;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Collection utility methods.
 * @author Paul Ferraro
 */
public class Collections
{
	public static final SortedSet<?> EMPTY_SORTED_SET = new EmptySortedSet<>();
	public static final SortedMap<?, ?> EMPTY_SORTED_MAP = new EmptySortedMap<>();
	static final Iterator<?> EMPTY_ITERATOR = new EmptyIterator<>();
	static final Comparator<?> NATURAL_COMPARATOR = new NaturalComparator<>();
	
	/**
	 * Returns an iterator over an empty collection.
	 * @param <E> the collection type
	 * @return a collection iterator
	 */
	@SuppressWarnings("unchecked")
	static <E> Iterator<E> emptyIterator()
	{
		return (Iterator<E>) EMPTY_ITERATOR;
	}

	/**
	 * Returns a comparator using the natural ordering of comparable objects.
	 * @param <E> the collection type
	 * @return a comparator
	 */
	@SuppressWarnings("unchecked")
	static <E> Comparator<E> naturalComparator()
	{
		return (Comparator<E>) NATURAL_COMPARATOR;
	}
	
	/**
	 * Like {@link java.util.Collections#emptySet()}, but returns a sorted set.
	 * @param <E> a collection type
	 * @return an empty sorted set
	 */
	@SuppressWarnings("unchecked")
	public static <E> SortedSet<E> emptySortedSet()
	{
		return (SortedSet<E>) EMPTY_SORTED_SET;
	}
	
	/**
	 * Like {@link java.util.Collections#emptyMap()}, but returns a sorted map.
	 * @param <K> the map's key type
	 * @param <V> the map's value type
	 * @return an empty sorted map
	 */
	@SuppressWarnings("unchecked")
	public static <K, V> SortedMap<K, V> emptySortedMap()
	{
		return (SortedMap<K, V>) EMPTY_SORTED_MAP;
	}
	
	/**
	 * Like {@link java.util.Collections#singleton(Object)}, but returns a sorted set.
	 * @param <E>
	 * @param element
	 * @return a sorted set containing the single element.
	 */
	public static <E> SortedSet<E> singletonSortedSet(E element)
	{
		return new SingletonSortedSet<>(element);
	}
	
	/**
	 * Like {@link java.util.Collections#singletonMap(Object, Object)}, but returns a sorted map.
	 * @param <K>
	 * @param <V>
	 * @param key
	 * @param value
	 * @return a sorted map containing the single key, value pair.
	 */
	public static <K, V> SortedMap<K, V> singletonSortedMap(K key, V value)
	{
		return new SingletonSortedMap<>(key, value);
	}

	static class NaturalComparator<E> implements Comparator<E>, Serializable
	{
		private static final long serialVersionUID = 984839581274651936L;

		@SuppressWarnings("unchecked")
		@Override
		public int compare(E object1, E object2)
		{
			return ((Comparable<E>) object1).compareTo(object2);
		}
	}

	static class EmptyIterator<E> implements Iterator<E>
	{
		@Override
		public boolean hasNext()
		{
			return false;
		}

		@Override
		public E next()
		{
			throw new NoSuchElementException();
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}
	
	static class EmptySortedSet<E> extends AbstractSet<E> implements SortedSet<E>, Serializable
	{
		private static final long serialVersionUID = 8614249160102450427L;

		@Override
		public Iterator<E> iterator()
		{
			return emptyIterator();
		}

		@Override
		public boolean contains(Object object)
		{
			return false;
		}

		@Override
		public boolean containsAll(Collection<?> c)
		{
			return c.isEmpty();
		}

		@Override
		public boolean isEmpty()
		{
			return true;
		}

		@Override
		public int size()
		{
			return 0;
		}

		@Override
		public Comparator<? super E> comparator()
		{
			return null;
		}

		@Override
		public E first()
		{
			throw new NoSuchElementException();
		}

		@Override
		public SortedSet<E> headSet(E toElement)
		{
			return emptySortedSet();
		}

		@Override
		public E last()
		{
			throw new NoSuchElementException();
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement)
		{
			return emptySortedSet();
		}

		@Override
		public SortedSet<E> tailSet(E fromElement)
		{
			return emptySortedSet();
		}
	}
	
	static class EmptySortedMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V>, Serializable
	{
		private static final long serialVersionUID = -7955186590936566806L;

		@Override
		public boolean containsKey(Object object)
		{
			return false;
		}

		@Override
		public boolean containsValue(Object object)
		{
			return false;
		}

		@Override
		public V get(Object object)
		{
			return null;
		}

		@Override
		public boolean isEmpty()
		{
			return true;
		}

		@Override
		public Set<K> keySet()
		{
			return java.util.Collections.emptySet();
		}

		@Override
		public int size()
		{
			return 0;
		}

		@Override
		public Collection<V> values()
		{
			return java.util.Collections.emptySet();
		}

		@Override
		public Set<java.util.Map.Entry<K, V>> entrySet()
		{
			return java.util.Collections.emptySet();
		}

		@Override
		public Comparator<? super K> comparator()
		{
			return null;
		}

		@Override
		public K firstKey()
		{
			throw new NoSuchElementException();
		}

		@Override
		public SortedMap<K, V> headMap(K toKey)
		{
			return emptySortedMap();
		}

		@Override
		public K lastKey()
		{
			throw new NoSuchElementException();
		}

		@Override
		public SortedMap<K, V> subMap(K fromKey, K toKey)
		{
			return emptySortedMap();
		}

		@Override
		public SortedMap<K, V> tailMap(K fromKey)
		{
			return emptySortedMap();
		}
	}
	
	static class SingletonIterator<E> implements Iterator<E>
	{
		private final E element;
		private boolean hasNext = true;
		
		SingletonIterator(E element)
		{
			this.element = element;
		}
		
		@Override
		public boolean hasNext()
		{
			return this.hasNext;
		}

		@Override
		public E next()
		{
			if (!this.hasNext)
			{
				throw new NoSuchElementException();
			}

			this.hasNext = false;
			
			return this.element;
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}
	
	static class NullComparator<E> implements Comparator<E>, Serializable
	{
		private static final long serialVersionUID = -8178822438438191299L;

		@Override
		public int compare(E object1, E object2)
		{
			return 0;
		}
	}
	
	static class SingletonSortedSet<E> extends AbstractSet<E> implements SortedSet<E>, Serializable
	{
		private static final long serialVersionUID = -7831170170325623175L;
		
		private final E element;
		
		SingletonSortedSet(E element)
		{
			this.element = element;
		}

		@Override
		public Iterator<E> iterator()
		{
			return new SingletonIterator<>(this.element);
		}

		@Override
		public int size()
		{
			return 1;
		}

		@Override
		public boolean contains(Object object)
		{
			return this.element.equals(object);
		}

		@Override
		public boolean isEmpty()
		{
			return false;
		}

		@Override
		public Comparator<? super E> comparator()
		{
			return null;
		}

		@Override
		public E first()
		{
			return this.element;
		}

		@Override
		public SortedSet<E> headSet(E toElement)
		{
			return (naturalComparator().compare(this.element, toElement) < 0) ? this : Collections.<E>emptySortedSet();
		}

		@Override
		public E last()
		{
			return this.element;
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement)
		{
			Comparator<E> comparator = naturalComparator();
			
			return ((comparator.compare(this.element, toElement) < 0) && (comparator.compare(this.element, fromElement) >= 0)) ? this : Collections.<E>emptySortedSet();
		}

		@Override
		public SortedSet<E> tailSet(E fromElement)
		{
			return (naturalComparator().compare(this.element, fromElement) >= 0) ? this : Collections.<E>emptySortedSet();
		}
	}
	
	static class SingletonSortedMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V>, Serializable
	{
		private static final long serialVersionUID = -3229163217706447957L;
		
		private final Entry<K, V> entry;
		
		SingletonSortedMap(K key, V value)
		{
			this.entry = new SimpleImmutableEntry<>(key, value);
		}

		@Override
		public boolean containsKey(Object object)
		{
			return this.entry.getKey().equals(object);
		}

		@Override
		public boolean containsValue(Object object)
		{
			return this.entry.getValue().equals(object);
		}

		@Override
		public V get(Object object)
		{
			return this.entry.getKey().equals(object) ? this.entry.getValue() : null;
		}

		@Override
		public boolean isEmpty()
		{
			return false;
		}

		@Override
		public Set<K> keySet()
		{
			return java.util.Collections.singleton(this.entry.getKey());
		}

		@Override
		public int size()
		{
			return 1;
		}

		@Override
		public Collection<V> values()
		{
			return java.util.Collections.singleton(this.entry.getValue());
		}

		@Override
		public Set<java.util.Map.Entry<K, V>> entrySet()
		{
			return java.util.Collections.singleton(this.entry);
		}

		@Override
		public Comparator<? super K> comparator()
		{
			return null;
		}

		@Override
		public K firstKey()
		{
			return this.entry.getKey();
		}

		@Override
		public SortedMap<K, V> headMap(K toKey)
		{
			return (naturalComparator().compare(this.entry.getKey(), toKey) < 0) ? this : Collections.<K, V>emptySortedMap();
		}

		@Override
		public K lastKey()
		{
			return this.entry.getKey();
		}

		@Override
		public SortedMap<K, V> subMap(K fromKey, K toKey)
		{
			Comparator<K> comparator = naturalComparator();
			
			return ((comparator.compare(this.entry.getKey(), toKey) < 0) && (comparator.compare(this.entry.getKey(), fromKey) >= 0)) ? this : Collections.<K, V>emptySortedMap();
		}

		@Override
		public SortedMap<K, V> tailMap(K fromKey)
		{
			return (naturalComparator().compare(this.entry.getKey(), fromKey) >= 0) ? this : Collections.<K, V>emptySortedMap();
		}
	}
	
	private Collections()
	{
		// Hide constructor
	}
}
