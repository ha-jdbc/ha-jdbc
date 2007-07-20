/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.util;

/**
 * @author Paul Ferraro
 */
public interface ClassEnum<T>
{
	public T newInstance() throws Exception;
	
	public boolean isInstance(T object);
}
