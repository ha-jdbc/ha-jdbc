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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.hajdbc.ConnectionProperties;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.cache.ColumnProperties;
import net.sf.hajdbc.util.Strings;

/**
 * Dialect for <a href="http://postgresql.org">PostgreSQL</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("nls")
public class PostgreSQLDialect extends StandardDialect implements DumpRestoreSupport
{
	private static final File PASSWORD_FILE = new File(String.format("%s%s.pgpass", Strings.USER_HOME, Strings.FILE_SEPARATOR));
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#vendor()
	 */
	@Override
	protected String vendorPattern()
	{
		return "postgresql";
	}

	/**
	 * PostgreSQL uses a schema search path to locate unqualified table names.
	 * The default search path is [$user,public], where $user is the current user.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		Connection connection = metaData.getConnection();
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SHOW search_path");
		
		resultSet.next();
		
		String[] schemas = resultSet.getString(1).split(Strings.COMMA);
		
		resultSet.close();
		statement.close();
		
		List<String> schemaList = new ArrayList<String>(schemas.length);
		
		for (String schema: schemas)
		{
			schemaList.add(schema.equals("$user") ? metaData.getUserName() : schema);
		}
		
		return schemaList;
	}

	/**
	 * PostgreSQL uses the native type OID to identify BLOBs.
	 * However the JDBC driver incomprehensibly maps OIDs to INTEGERs.
	 * The PostgreSQL JDBC folks claim this intentional.
	 * @see net.sf.hajdbc.dialect.StandardDialect#getColumnType(net.sf.hajdbc.cache.ColumnProperties)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getNativeType().equalsIgnoreCase("oid") ? Types.BLOB : properties.getType();
	}

	/**
	 * Versions &gt;=8.1 of the PostgreSQL JDBC driver return incorrect values for DatabaseMetaData.getExtraNameCharacters().
	 * @see net.sf.hajdbc.dialect.StandardDialect#getIdentifierPattern(java.sql.DatabaseMetaData)
	 */
	@Override
	public Pattern getIdentifierPattern(DatabaseMetaData metaData) throws SQLException
	{
		if ((metaData.getDriverMajorVersion() >= 8) && (metaData.getDriverMinorVersion() >= 1))
		{
			return Pattern.compile("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*");
		}
		
		return super.getIdentifierPattern(metaData);
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(?:CURR|NEXT)VAL\\s*\\(\\s*'([^']+)'\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#nextSequenceValueFormat()
	 */
	@Override
	protected String nextSequenceValueFormat()
	{
		return "NEXTVAL(''{0}'')";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#alterIdentityColumnFormat()
	 */
	@Override
	protected String alterIdentityColumnFormat()
	{
		return "ALTER SEQUENCE {0}_{1}_seq RESTART WITH {2}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return super.currentTimestampPattern() + "|(?<=\\W)NOW\\s*\\(\\s*\\)|(?<=\\W)TRANSACTION_TIMESTAMP\\s*\\(\\s*\\)|(?<=\\W)STATEMENT_TIMESTAMP\\s*\\(\\s*\\)|(?<=\\W)CLOCK_TIMESTAMP\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#randomPattern()
	 */
	@Override
	protected String randomPattern()
	{
		return "(?<=\\W)RANDOM\\s*\\(\\s*\\)";
	}

	/**
	 * Recognizes FOR SHARE and FOR UPDATE.
	 * @see net.sf.hajdbc.dialect.StandardDialect#selectForUpdatePattern()
	 */
	@Override
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+FOR\\s+(SHARE|UPDATE)";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDumpRestoreSupport()
	 */
	@Override
	public DumpRestoreSupport getDumpRestoreSupport()
	{
		return this;
	}

	/**
	 * {@inheritDoc}
	 * @throws IOException 
	 * @see net.sf.hajdbc.dialect.StandardDialect#startDumpProcess(java.lang.String, java.io.File)
	 */
	@Override
	public ProcessBuilder createDumpProcess(ConnectionProperties properties, File file)
	{
		return this.setPassword(new ProcessBuilder("pg_dump", "-h", properties.getHost(), "-p", properties.getPort(), "-U", properties.getUser(), "-f", file.getPath(), "-F", "tar", properties.getDatabase()), properties);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#startRestoreProcess(java.lang.String, java.io.File)
	 */
	@Override
	public ProcessBuilder createRestoreProcess(ConnectionProperties properties, File file)
	{
		return this.setPassword(new ProcessBuilder("pg_restore", "-h", properties.getHost(), "-p", properties.getPort(), "-U", properties.getUser(), "-d", properties.getDatabase(), file.getPath()), properties);
	}
	
	private ProcessBuilder setPassword(ProcessBuilder builder, ConnectionProperties properties)
	{
		if (!PASSWORD_FILE.exists())
		{
			builder.environment().put("PGPASSWORD", properties.getPassword());
		}
		
		return builder;
	}
}
