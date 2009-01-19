/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.dialect;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.util.ClassEnum;
import net.sf.hajdbc.util.Enums;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public enum DialectClass implements ClassEnum<Dialect>
{
	DB2(DB2Dialect.class),
	DERBY(DerbyDialect.class),
	FIREBIRD(FirebirdDialect.class),
	H2(H2Dialect.class),
	HSQLDB(HSQLDBDialect.class),
	INGRES(IngresDialect.class),
	MAXDB(MaxDBDialect.class),
	MCKOI(MckoiDialect.class),
	MYSQL(MySQLDialect.class),
	ORACLE(OracleDialect.class),
	POSTGRESQL(PostgreSQLDialect.class),
	STANDARD(StandardDialect.class),
	SYBASE(SybaseDialect.class);
	
	private Class<? extends Dialect> dialectClass;
	
	private DialectClass(Class<? extends Dialect> dialectClass)
	{
		this.dialectClass = dialectClass;
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#isInstance(java.lang.Object)
	 */
	@Override
	public boolean isInstance(Dialect dialect)
	{
		return this.dialectClass.equals(dialect.getClass());
	}

	/**
	 * @see net.sf.hajdbc.util.ClassEnum#newInstance()
	 */
	@Override
	public Dialect newInstance() throws Exception
	{
		return this.dialectClass.newInstance();
	}

	/**
	 * Creates a new instance of the Dialect implementation from the specified class name.
	 * @param id the class name of a Dialect instance.
	 * @return a new Dialect instance
	 * @throws Exception if a Dialect instance could not be instantiated from the specified class name.
	 */
	public static Dialect deserialize(String id) throws Exception
	{
		try
		{
			DialectClass dialectClass = (id != null) ? Enums.valueOf(DialectClass.class, id) : STANDARD;
			
			return dialectClass.newInstance();
		}
		catch (IllegalArgumentException e)
		{
			return Class.forName(id).asSubclass(Dialect.class).newInstance();
		}
	}
	
	/**
	 * Return a String representation that identifies the specified Dialect.
	 * @param dialect a Dialect implementation
	 * @return the class name of this dialect
	 */
	public static String serialize(Dialect dialect)
	{
		for (DialectClass dialectClass: DialectClass.values())
		{
			if (dialectClass.isInstance(dialect))
			{
				return Enums.id(dialectClass);
			}
		}
		
		return dialect.getClass().getName();
	}
}
