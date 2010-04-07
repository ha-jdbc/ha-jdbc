/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-2010 Paul Ferraro
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
 * @author paul
 *
 */
public class Collections
{
	public static final SortedSet<?> EMPTY_SORTED_SET = new EmptySortedSet<Object>();
	public static final SortedMap<?, ?> EMPTY_SORTED_MAP = new EmptySortedMap<Object, Object>();
	static final Iterator<?> EMPTY_ITERATOR = new EmptyIterator<Object>();
	static final Comparator<?> NULL_COMPARATOR = new NullComparator<Object>();
	
	private Collections()
	{
		// Hide constructor
	}
	
	@SuppressWarnings("unchecked")
	public static <E> SortedSet<E> emptySortedSet()
	{
		return (SortedSet<E>) EMPTY_SORTED_SET;
	}
	
	@SuppressWarnings("unchecked")
	public static <K, V> SortedMap<K, V> emptySortedMap()
	{
		return (SortedMap<K, V>) EMPTY_SORTED_MAP;
	}
	
	/**
	 * Like {@link java.util.Collections#singleton(Object)}, but returns a {@link SortedSet}.
	 * @param <E>
	 * @param element
	 * @return
	 */
	public static <E> SortedSet<E> singletonSortedSet(E element)
	{
		return new SingletonSortedSet<E>(element);
	}
	
	/**
	 * Like {@link java.util.Collections#singletonMap(Object, Object)}, but returns a {@link SortedMap}.
	 * @param <K>
	 * @param <V>
	 * @param element
	 * @return
	 */
	public static <K, V> SortedMap<K, V> singletonSortedMap(K key, V value)
	{
		return new SingletonSortedMap<K, V>(key, value);
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

		@SuppressWarnings("unchecked")
		@Override
		public Iterator<E> iterator()
		{
			return (Iterator<E>) EMPTY_ITERATOR;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractCollection#contains(java.lang.Object)
		 */
		@Override
		public boolean contains(Object object)
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractCollection#containsAll(java.util.Collection)
		 */
		@Override
		public boolean containsAll(Collection<?> c)
		{
			return c.isEmpty();
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractCollection#isEmpty()
		 */
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

		@SuppressWarnings("unchecked")
		@Override
		public Comparator<? super E> comparator()
		{
			return (Comparator<E>) NULL_COMPARATOR;
		}

		@Override
		public E first()
		{
			return null;
		}

		@Override
		public SortedSet<E> headSet(E arg0)
		{
			return null;
		}

		@Override
		public E last()
		{
			return null;
		}

		@Override
		public SortedSet<E> subSet(E arg0, E arg1)
		{
			return null;
		}

		@Override
		public SortedSet<E> tailSet(E arg0)
		{
			return null;
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

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#isEmpty()
		 */
		@Override
		public boolean isEmpty()
		{
			return true;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#keySet()
		 */
		@Override
		public Set<K> keySet()
		{
			return java.util.Collections.emptySet();
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#size()
		 */
		@Override
		public int size()
		{
			return 0;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#values()
		 */
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

		@SuppressWarnings("unchecked")
		@Override
		public Comparator<? super K> comparator()
		{
			return (Comparator<K>) NULL_COMPARATOR;
		}

		@Override
		public K firstKey()
		{
			return null;
		}

		@Override
		public SortedMap<K, V> headMap(K toKey)
		{
			return null;
		}

		@Override
		public K lastKey()
		{
			return null;
		}

		@Override
		public SortedMap<K, V> subMap(K fromKey, K toKey)
		{
			return null;
		}

		@Override
		public SortedMap<K, V> tailMap(K fromKey)
		{
			return null;
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
	
	static class NullComparator<E> implements Comparator<E>
	{
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
			return new SingletonIterator<E>(this.element);
		}

		@Override
		public int size()
		{
			return 1;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractCollection#contains(java.lang.Object)
		 */
		@Override
		public boolean contains(Object object)
		{
			return this.element.equals(object);
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractCollection#isEmpty()
		 */
		@Override
		public boolean isEmpty()
		{
			return false;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Comparator<? super E> comparator()
		{
			return (Comparator<E>) NULL_COMPARATOR;
		}

		@Override
		public E first()
		{
			return this.element;
		}

		@Override
		public SortedSet<E> headSet(E toElement)
		{
			return null;
		}

		@Override
		public E last()
		{
			return this.element;
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement)
		{
			return null;
		}

		@Override
		public SortedSet<E> tailSet(E fromElement)
		{
			return null;
		}
	}
	
	static class SingletonSortedMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V>, Serializable
	{
		private static final long serialVersionUID = -3229163217706447957L;
		
		private final Entry<K, V> entry;
		
		SingletonSortedMap(K key, V value)
		{
			this.entry = new SimpleImmutableEntry<K, V>(key, value);
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#containsKey(java.lang.Object)
		 */
		@Override
		public boolean containsKey(Object object)
		{
			return this.entry.getKey().equals(object);
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#containsValue(java.lang.Object)
		 */
		@Override
		public boolean containsValue(Object object)
		{
			return this.entry.getValue().equals(object);
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#get(java.lang.Object)
		 */
		@Override
		public V get(Object object)
		{
			return this.entry.getKey().equals(object) ? this.entry.getValue() : null;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#isEmpty()
		 */
		@Override
		public boolean isEmpty()
		{
			return false;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#keySet()
		 */
		@Override
		public Set<K> keySet()
		{
			return java.util.Collections.singleton(this.entry.getKey());
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#size()
		 */
		@Override
		public int size()
		{
			return 1;
		}

		/**
		 * {@inheritDoc}
		 * @see java.util.AbstractMap#values()
		 */
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

		@SuppressWarnings("unchecked")
		@Override
		public Comparator<? super K> comparator()
		{
			return (Comparator<K>) NULL_COMPARATOR;
		}

		@Override
		public K firstKey()
		{
			return this.entry.getKey();
		}

		@Override
		public SortedMap<K, V> headMap(K toKey)
		{
			return null;
		}

		@Override
		public K lastKey()
		{
			return this.entry.getKey();
		}

		@Override
		public SortedMap<K, V> subMap(K fromKey, K toKey)
		{
			return null;
		}

		@Override
		public SortedMap<K, V> tailMap(K fromKey)
		{
			return null;
		}
	}
}
