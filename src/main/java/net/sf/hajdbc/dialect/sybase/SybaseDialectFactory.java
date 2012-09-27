package net.sf.hajdbc.dialect.sybase;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class SybaseDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 93487345688500431L;

	@Override
	public String getId()
	{
		return "sybase";
	}

	@Override
	public Dialect createDialect()
	{
		return new SybaseDialect();
	}
}
