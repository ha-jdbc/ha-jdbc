package net.sf.hajdbc.dialect.ingres;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class IngresDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -3317286873687123210L;

	@Override
	public String getId()
	{
		return "ingres";
	}

	@Override
	public Dialect createDialect()
	{
		return new IngresDialect();
	}
}
