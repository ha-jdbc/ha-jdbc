/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.cache;

import net.sf.hajdbc.SequenceProperties;

/**
 * @author Paul Ferraro
 */
public class SequencePropertiesImpl implements SequenceProperties
{
	private String name;
	
	public SequencePropertiesImpl(String name)
	{
		this.name = name;
	}
	
	/**
	 * @see net.sf.hajdbc.SequenceProperties#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}
}
