package net.sf.hajdbc.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

public class CollectionsTest
{
	@Test
	public void emptySortedSet()
	{
		this.verify(Collections.<Integer>emptySortedSet(), new TreeSet<Integer>(), 0, 1);
	}
	
	@Test
	public void singletonSortedSet()
	{
		SortedSet<Integer> set = new TreeSet<Integer>();
		set.add(1);
		
		this.verify(Collections.<Integer>singletonSortedSet(1), set, 0, 1);
	}
	
	private <T> void verify(SortedSet<T> immutableSet, SortedSet<T> mutableSet, T value1, T value2)
	{
		try
		{
			immutableSet.add(value1);
			Assert.fail();
		}
		catch (UnsupportedOperationException e)
		{
		}

		try
		{
			immutableSet.addAll(java.util.Collections.singleton(value1));
			Assert.fail();
		}
		catch (UnsupportedOperationException e)
		{
		}

		try
		{
			immutableSet.clear();
			
			Assert.assertTrue(mutableSet.isEmpty());
		}
		catch (UnsupportedOperationException e)
		{
			Assert.assertFalse(mutableSet.isEmpty());
		}
		
		Assert.assertEquals(immutableSet.contains(value1), mutableSet.contains(value1));
		Assert.assertEquals(immutableSet.contains(value2), mutableSet.contains(value2));
		
		Assert.assertEquals(immutableSet.containsAll(java.util.Collections.emptySet()), mutableSet.containsAll(java.util.Collections.emptySet()));
		Assert.assertEquals(immutableSet.containsAll(java.util.Collections.singleton(0)), mutableSet.containsAll(java.util.Collections.singleton(0)));
		Assert.assertEquals(immutableSet.containsAll(java.util.Collections.singleton(1)), mutableSet.containsAll(java.util.Collections.singleton(1)));
		
		Iterator<T> iterator = immutableSet.iterator();

		if (!mutableSet.isEmpty())
		{
			Assert.assertTrue(iterator.hasNext());
			
			Assert.assertEquals(iterator.next(), mutableSet.iterator().next());			
		}

		Assert.assertFalse(iterator.hasNext());
		
		try
		{
			iterator.next();
			Assert.fail();
		}
		catch (NoSuchElementException e)
		{
		}
		
		Assert.assertEquals(immutableSet.isEmpty(), mutableSet.isEmpty());
		
		try
		{
			immutableSet.remove(value1);
		}
		catch (UnsupportedOperationException e)
		{
		}
		
		try
		{
			immutableSet.removeAll(java.util.Collections.singleton(value1));
		}
		catch (UnsupportedOperationException e)
		{
		}
		
		try
		{
			immutableSet.retainAll(java.util.Collections.singleton(value1));
		}
		catch (UnsupportedOperationException e)
		{
		}
		
		Assert.assertEquals(immutableSet.size(), mutableSet.size());
		
		Assert.assertArrayEquals(immutableSet.toArray(), mutableSet.toArray());
		
		Assert.assertSame(immutableSet.comparator(), mutableSet.comparator());
		
		if (!mutableSet.isEmpty())
		{
			Assert.assertEquals(immutableSet.first(), mutableSet.first());
			Assert.assertEquals(immutableSet.last(), mutableSet.last());
		}
		else
		{
			try
			{
				immutableSet.first();
				Assert.fail();
			}
			catch (NoSuchElementException e)
			{
			}
			
			try
			{
				immutableSet.last();
				Assert.fail();
			}
			catch (NoSuchElementException e)
			{
			}
		}
		
		Assert.assertEquals(immutableSet.headSet(value1), mutableSet.headSet(value1));
		Assert.assertEquals(immutableSet.headSet(value2), mutableSet.headSet(value2));

		Assert.assertEquals(immutableSet.subSet(value1, value2), mutableSet.subSet(value1, value2));
		
		Assert.assertEquals(immutableSet.tailSet(value1), mutableSet.tailSet(value1));
		Assert.assertEquals(immutableSet.tailSet(value2), mutableSet.tailSet(value2));
	}
	
	@Test
	public void emptySortedMap()
	{
		this.verify(Collections.<Integer, String>emptySortedMap(), new TreeMap<Integer, String>(), 0, 1, "");
	}
	
	@Test
	public void singletonSortedMap()
	{
		SortedMap<Integer, String> map = new TreeMap<Integer, String>();
		map.put(1, "");
		
		this.verify(Collections.<Integer, String>singletonSortedMap(1, ""), map, 0, 1, "");
	}
	
	private <K, V> void verify(SortedMap<K, V> immutableMap, SortedMap<K, V> mutableMap, K key1, K key2, V value)
	{
		try
		{
			immutableMap.clear();
			
			Assert.assertTrue(mutableMap.isEmpty());
		}
		catch (UnsupportedOperationException e)
		{
			Assert.assertFalse(mutableMap.isEmpty());
		}
		
		Assert.assertEquals(immutableMap.containsKey(key1), mutableMap.containsKey(key1));
		Assert.assertEquals(immutableMap.containsKey(key2), mutableMap.containsKey(key2));

		Assert.assertEquals(immutableMap.containsValue(value), mutableMap.containsValue(value));
		Assert.assertEquals(immutableMap.containsValue(null), mutableMap.containsValue(null));
		
		Assert.assertEquals(immutableMap.entrySet(), mutableMap.entrySet());
		
		Assert.assertEquals(immutableMap.get(key1), mutableMap.get(key1));
		Assert.assertEquals(immutableMap.get(key2), mutableMap.get(key2));
		
		Assert.assertEquals(immutableMap.isEmpty(), mutableMap.isEmpty());
		
		Assert.assertEquals(immutableMap.keySet(), mutableMap.keySet());
		
		try
		{
			immutableMap.put(key1, value);
			Assert.fail();
		}
		catch (UnsupportedOperationException e)
		{
		}

		try
		{
			immutableMap.putAll(java.util.Collections.singletonMap(key1, value));
			Assert.fail();
		}
		catch (UnsupportedOperationException e)
		{
		}

		try
		{
			immutableMap.remove(key1);
		}
		catch (UnsupportedOperationException e)
		{
		}
		
		Assert.assertEquals(immutableMap.size(), mutableMap.size());
		
		// Is it OK that this fails?
		// Assert.assertEquals(immutableMap.values(), mutableMap.values());
		Assert.assertEquals(immutableMap.values().size(), mutableMap.values().size());
		if (!mutableMap.isEmpty())
		{
			Assert.assertEquals(immutableMap.values().iterator().next(), mutableMap.values().iterator().next());
		}
		
		Assert.assertSame(immutableMap.comparator(), mutableMap.comparator());
		
		if (!mutableMap.isEmpty())
		{
			Assert.assertEquals(immutableMap.firstKey(), mutableMap.firstKey());
			Assert.assertEquals(immutableMap.lastKey(), mutableMap.lastKey());
		}
		else
		{
			try
			{
				immutableMap.firstKey();
				Assert.fail();
			}
			catch (NoSuchElementException e)
			{
			}
			
			try
			{
				immutableMap.lastKey();
				Assert.fail();
			}
			catch (NoSuchElementException e)
			{
			}
		}
		
		Assert.assertEquals(immutableMap.headMap(key1), mutableMap.headMap(key1));
		Assert.assertEquals(immutableMap.headMap(key2), mutableMap.headMap(key2));

		Assert.assertEquals(immutableMap.subMap(key1, key2), mutableMap.subMap(key1, key2));
		
		Assert.assertEquals(immutableMap.tailMap(key1), mutableMap.tailMap(key1));
		Assert.assertEquals(immutableMap.tailMap(key2), mutableMap.tailMap(key2));
	}
}
