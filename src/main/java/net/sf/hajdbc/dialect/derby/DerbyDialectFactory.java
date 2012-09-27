package net.sf.hajdbc.dialect.derby;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class DerbyDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = -1482749134479553618L;

	@Override
	public String getId()
	{
		return "derby";
	}

	@Override
	public Dialect createDialect()
	{
		return new DerbyDialect();
	}
}
