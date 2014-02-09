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
package net.sf.hajdbc.util.ref;

import java.lang.ref.Reference;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A map that stores its values using a reference.
 * @author Paul Ferraro
 */
public class ReferenceMap<K, V> implements Map<K, V>
{
	private final Map<K, Reference<V>> map;
	final ReferenceFactory referenceFactory;
	
	public ReferenceMap(Map<K, Reference<V>> map, ReferenceFactory referenceFactory)
	{
		this.map = map;
		this.referenceFactory = referenceFactory;
	}

	@Override
	public void clear()
	{
		this.map.clear();
	}

	@Override
	public boolean containsKey(Object key)
	{
		return this.map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object object)
	{
		for (Reference<V> reference: this.map.values())
		{
			V value = reference.get();
			
			if ((value != null) && value.equals(object))
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet()
	{
		final Set<Map.Entry<K, Reference<V>>> entrySet = this.map.entrySet();
		
		return new Set<Map.Entry<K, V>>()
		{
			@Override
			public boolean add(Map.Entry<K, V> entry)
			{
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean addAll(Collection<? extends Map.Entry<K, V>> entries)
			{
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear()
			{
				entrySet.clear();
			}

			@Override
			public boolean contains(Object object)
			{
				return false;
			}

			@Override
			public boolean containsAll(Collection<?> collection)
			{
				return false;
			}

			@Override
			public boolean isEmpty()
			{
				return entrySet.isEmpty();
			}

			@Override
			public Iterator<Map.Entry<K, V>> iterator()
			{
				final Iterator<Map.Entry<K, Reference<V>>> entries = entrySet.iterator();
				
				return new Iterator<Map.Entry<K, V>>()
				{
					@Override
					public boolean hasNext()
					{
						return entries.hasNext();
					}

					@Override
					public java.util.Map.Entry<K, V> next()
					{
						final Map.Entry<K, Reference<V>> entry = entries.next();
						
						return new Map.Entry<K, V>()
						{
							@Override
							public K getKey()
							{
								return entry.getKey();
							}

							@Override
							public V getValue()
							{
								return entry.getValue().get();
							}

							@Override
							public V setValue(V value)
							{
								Reference<V> reference = entry.setValue(ReferenceMap.this.referenceFactory.createReference(value));
								
								return (reference != null) ? reference.get() : null;
							}
						};
					}

					@Override
					public void remove()
					{
						entries.remove();
					}
				};
			}

			@Override
			public boolean remove(Object object)
			{
				return false;
			}

			@Override
			public boolean removeAll(Collection<?> collection)
			{
				return false;
			}

			@Override
			public boolean retainAll(Collection<?> collection)
			{
				return false;
			}

			@Override
			public int size()
			{
				return entrySet.size();
			}

			@Override
			public Object[] toArray()
			{
				return null;
			}

			@Override
			public <T> T[] toArray(T[] arg0)
			{
				return null;
			}
		};
	}

	@Override
	public V get(Object key)
	{
		Reference<V> reference = this.map.get(key);
		
		return (reference != null) ? reference.get() : null;
	}

	@Override
	public boolean isEmpty()
	{
		return this.map.isEmpty();
	}

	@Override
	public Set<K> keySet()
	{
		return this.map.keySet();
	}

	@Override
	public V put(K key, V value)
	{
		Reference<V> reference = this.map.put(key, this.referenceFactory.createReference(value));
		
		return (reference != null) ? reference.get() : null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map)
	{
		for (Map.Entry<? extends K, ? extends V> entry: map.entrySet())
		{
			this.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public V remove(Object key)
	{
		Reference<V> reference = this.map.remove(key);
		
		return (reference != null) ? reference.get() : null;
	}

	@Override
	public int size()
	{
		return this.map.size();
	}

	@Override
	public Collection<V> values()
	{
		final Collection<Reference<V>> references = this.map.values();
		
		return new Collection<V>()
		{
			@Override
			public boolean add(V arg0)
			{
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean addAll(Collection<? extends V> arg0)
			{
				throw new UnsupportedOperationException();
			}

			@Override
			public void clear()
			{
				references.clear();
			}

			@Override
			public boolean contains(Object object)
			{
				for (Reference<V> reference: references)
				{
					V value = reference.get();
					
					if ((value != null) && value.equals(object))
					{
						return true;
					}
				}
				
				return false;
			}

			@Override
			public boolean containsAll(Collection<?> objects)
			{
				return false;
			}

			@Override
			public boolean isEmpty()
			{
				return references.isEmpty();
			}

			@Override
			public Iterator<V> iterator()
			{
				final Iterator<Reference<V>> refs = references.iterator();
				
				return new Iterator<V>()
				{
					@Override
					public boolean hasNext()
					{
						return refs.hasNext();
					}

					@Override
					public V next()
					{
						Reference<V> reference = refs.next();
						
						return (reference != null) ? reference.get() : null;
					}

					@Override
					public void remove()
					{
						refs.remove();
					}
				};
			}

			@Override
			public boolean remove(Object value)
			{
				return false;
			}

			@Override
			public boolean removeAll(Collection<?> values)
			{
				return false;
			}

			@Override
			public boolean retainAll(Collection<?> values)
			{
				return false;
			}

			@Override
			public int size()
			{
				return references.size();
			}

			@Override
			public Object[] toArray()
			{
				return null;
			}

			@Override
			public <T> T[] toArray(T[] arg0)
			{
				return null;
			}
		};
	}
}
