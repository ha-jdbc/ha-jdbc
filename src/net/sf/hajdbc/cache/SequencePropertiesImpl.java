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

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof SequenceProperties)) return false;
		
		SequenceProperties sequence = (SequenceProperties) object;
		
		return this.name.equals(sequence.getName());
	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.name;
	}
}
