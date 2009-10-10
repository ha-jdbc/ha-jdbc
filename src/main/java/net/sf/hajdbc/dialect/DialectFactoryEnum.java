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

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public enum DialectFactoryEnum implements DialectFactory
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
	
	private DialectFactoryEnum(Class<? extends Dialect> dialectClass)
	{
		this.dialectClass = dialectClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.DialectFactory#createDialect()
	 */
	@Override
	public Dialect createDialect()
	{
		try
		{
			return this.dialectClass.newInstance();
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException(e);
		}
	}
}
