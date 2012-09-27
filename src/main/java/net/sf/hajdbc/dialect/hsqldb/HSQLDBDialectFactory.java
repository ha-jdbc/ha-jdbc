package net.sf.hajdbc.dialect.hsqldb;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class HSQLDBDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 1061788303412941491L;

	@Override
	public String getId()
	{
		return "hsqldb";
	}

	@Override
	public Dialect createDialect()
	{
		return new HSQLDBDialect();
	}
}
