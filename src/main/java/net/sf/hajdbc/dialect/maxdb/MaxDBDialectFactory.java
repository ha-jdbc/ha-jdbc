package net.sf.hajdbc.dialect.maxdb;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class MaxDBDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 7571104466555331544L;

	@Override
	public String getId()
	{
		return "maxdb";
	}

	@Override
	public Dialect createDialect()
	{
		return new MaxDBDialect();
	}
}
