package net.sf.hajdbc.dialect;

import net.sf.hajdbc.Dialect;

public interface DialectFactory
{
	Dialect createDialect();
}
