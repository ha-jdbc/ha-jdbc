/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Dialect;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
@XmlEnum(String.class)
@XmlType(name = "dialect")
public enum DialectFactoryEnum implements DialectFactory
{
	@XmlEnumValue("db2")
	DB2(DB2Dialect.class),
	@XmlEnumValue("derby")
	DERBY(DerbyDialect.class),
	@XmlEnumValue("firebird")
	FIREBIRD(FirebirdDialect.class),
	@XmlEnumValue("h2")
	H2(H2Dialect.class),
	@XmlEnumValue("hsqldb")
	HSQLDB(HSQLDBDialect.class),
	@XmlEnumValue("ingres")
	INGRES(IngresDialect.class),
	@XmlEnumValue("maxdb")
	MAXDB(MaxDBDialect.class),
	@XmlEnumValue("mckoi")
	MCKOI(MckoiDialect.class),
	@XmlEnumValue("mysql")
	MYSQL(MySQLDialect.class),
	@XmlEnumValue("oracle")
	ORACLE(OracleDialect.class),
	@XmlEnumValue("postgresql")
	POSTGRESQL(PostgreSQLDialect.class),
	@XmlEnumValue("standard")
	STANDARD(StandardDialect.class),
	@XmlEnumValue("sybase")
	SYBASE(SybaseDialect.class);
	
	private Class<? extends Dialect> targetClass;
	
	private DialectFactoryEnum(Class<? extends Dialect> targetClass)
	{
		this.targetClass = targetClass;
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
			return this.targetClass.newInstance();
		}
		catch (Exception e)
		{
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Enum#toString()
	 */
	@Override
	public String toString()
	{
		return this.name().toLowerCase();
	}
}
