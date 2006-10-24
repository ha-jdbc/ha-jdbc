/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Dialect for <a href="http://opensource.ingres.com/projects/ingres/">Ingres</a>.
 * 
 * @author Paul Ferraro
 */
public class IngresDialect extends StandardDialect
{
	private Pattern legacySequencePattern = Pattern.compile("(\\S+)\\.(?:(?:CURR)|(?:NEXT))VAL", Pattern.CASE_INSENSITIVE);
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#supportsIdentityColumns()
	 */
	@Override
	public boolean supportsIdentityColumns()
	{
		return false;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		List<String> sequenceList = new LinkedList<String>();
		
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT seq_name FROM iisequence");
		
		while (resultSet.next())
		{
			sequenceList.add(resultSet.getString(1));
		}
		
		resultSet.close();
		statement.close();
		
		return sequenceList;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#parseSequence(java.lang.String)
	 */
	@Override
	public String parseSequence(String sql)
	{
		String sequence = super.parseSequence(sql);
		
		return (sequence != null) ? sequence : this.parse(this.legacySequencePattern, sql);
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:(?:NEXT)|(?:CURRENT))\\s+VALUE\\s+FOR\\s+(\\S+)";
	}
}
