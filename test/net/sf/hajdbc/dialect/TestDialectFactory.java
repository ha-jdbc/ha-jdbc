/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestDialectFactory
{
	public void testSerialize()
	{
		Dialect dialect = EasyMock.createMock(Dialect.class);
		
		String id = DialectFactory.serialize(dialect);
		
		assert id.equals(dialect.getClass().getName()) : id;
	}
	
	public void testDeserialize()
	{
		this.assertDialect("net.sf.hajdbc.dialect.DefaultDialect", DefaultDialect.class);
		this.assertDialect("db2", DB2Dialect.class);
		this.assertDialect("derby", DerbyDialect.class);
		this.assertDialect("firebird", DefaultDialect.class);
		this.assertDialect("hsqldb", HSQLDBDialect.class);
		this.assertDialect("ingres", DefaultDialect.class);
		this.assertDialect("maxdb", MaxDBDialect.class);
		this.assertDialect("mckoi", DefaultDialect.class);
		this.assertDialect("mysql", DefaultDialect.class);
		this.assertDialect("oracle", MaxDBDialect.class);
		this.assertDialect("postgresql", PostgreSQLDialect.class);

		this.assertDialect("PostgreSQL", PostgreSQLDialect.class);
		this.assertDialect("POSTGRESQL", PostgreSQLDialect.class);

		try
		{
			Dialect dialect = DialectFactory.deserialize("invalid");
			
			assert false : dialect.getClass().getName();
		}
		catch (Exception e)
		{
			assert true;
		}
	}
	
	private void assertDialect(String id, Class<? extends Dialect> dialectClass)
	{
		try
		{
			Dialect dialect = DialectFactory.deserialize(id);
			
			assert dialectClass.isInstance(dialect) : dialect.getClass().getName();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
}
