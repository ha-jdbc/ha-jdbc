package net.sf.hajdbc.dialect.h2;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class H2DialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 9138552172726016094L;

	@Override
	public String getId()
	{
		return "h2";
	}

	@Override
	public Dialect createDialect()
	{
		return new H2Dialect();
	}
}
