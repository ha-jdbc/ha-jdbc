package net.sf.hajdbc.dialect.firebird;

import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.dialect.DialectFactory;

public class FirebirdDialectFactory implements DialectFactory
{
	private static final long serialVersionUID = 8753385208104801124L;

	@Override
	public String getId()
	{
		return "firebird";
	}

	@Override
	public Dialect createDialect()
	{
		return new FirebirdDialect();
	}
}
