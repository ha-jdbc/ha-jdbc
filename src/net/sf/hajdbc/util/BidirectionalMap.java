/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.util;

import java.util.Map;

/**
 * @author Paul Ferraro
 *
 */
public interface BidirectionalMap<K, V> extends Map<K, V>
{
	public K getKey(Object value);
	
	public K removeValue(Object value);
}
