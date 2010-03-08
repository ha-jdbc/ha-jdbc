package net.sf.hajdbc.dialect;

import java.io.Serializable;

import net.sf.hajdbc.Dialect;

public class CustomDialectFactory implements DialectFactory, Serializable
{
	private static final long serialVersionUID = -7879147571446129555L;
	
	private final Class<? extends Dialect> targetClass;
	
	public CustomDialectFactory(Class<? extends Dialect> targetClass)
	{
		this.targetClass = targetClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.DialectFactory#createDialect()
	 */
	@Override
	public Dialect createDialect()
	{
		try
		{
			return this.targetClass.newInstance();
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.targetClass.getName();
	}
}
