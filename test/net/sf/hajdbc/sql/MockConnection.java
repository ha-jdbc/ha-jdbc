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
package net.sf.hajdbc.sql;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.easymock.EasyMock;

/**
 * Mock connection that creates mock statements
 * @author  Paul Ferraro
 * @since   1.1
 */
public class MockConnection implements Connection
{
	/**
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement()
	{
		return EasyMock.createMock(Statement.class);
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(String arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(String arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public String nativeSQL(String arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(boolean arg0)
	{
	}

	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit()
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#commit()
	 */
	public void commit()
	{
	}

	/**
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback()
	{
	}

	/**
	 * @see java.sql.Connection#close()
	 */
	public void close()
	{
	}

	/**
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed()
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData()
	{
		return new DatabaseMetaData()
		{

			public boolean allProceduresAreCallable()
			{
				return false;
			}

			public boolean allTablesAreSelectable()
			{
				return false;
			}

			public String getURL()
			{
				return null;
			}

			public String getUserName()
			{
				return null;
			}

			public boolean isReadOnly()
			{
				return false;
			}

			public boolean nullsAreSortedHigh()
			{
				return false;
			}

			public boolean nullsAreSortedLow()
			{
				return false;
			}

			public boolean nullsAreSortedAtStart()
			{
				return false;
			}

			public boolean nullsAreSortedAtEnd()
			{
				return false;
			}

			public String getDatabaseProductName()
			{
				return null;
			}

			public String getDatabaseProductVersion()
			{
				return null;
			}

			public String getDriverName()
			{
				return null;
			}

			public String getDriverVersion()
			{
				return null;
			}

			public int getDriverMajorVersion()
			{
				return 0;
			}

			public int getDriverMinorVersion()
			{
				return 0;
			}

			public boolean usesLocalFiles()
			{
				return false;
			}

			public boolean usesLocalFilePerTable()
			{
				return false;
			}

			public boolean supportsMixedCaseIdentifiers()
			{
				return false;
			}

			public boolean storesUpperCaseIdentifiers()
			{
				return false;
			}

			public boolean storesLowerCaseIdentifiers()
			{
				return false;
			}

			public boolean storesMixedCaseIdentifiers()
			{
				return false;
			}

			public boolean supportsMixedCaseQuotedIdentifiers()
			{
				return false;
			}

			public boolean storesUpperCaseQuotedIdentifiers()
			{
				return false;
			}

			public boolean storesLowerCaseQuotedIdentifiers()
			{
				return false;
			}

			public boolean storesMixedCaseQuotedIdentifiers()
			{
				return false;
			}

			public String getIdentifierQuoteString()
			{
				return null;
			}

			public String getSQLKeywords()
			{
				return "";
			}

			public String getNumericFunctions()
			{
				return null;
			}

			public String getStringFunctions()
			{
				return null;
			}

			public String getSystemFunctions()
			{
				return null;
			}

			public String getTimeDateFunctions()
			{
				return null;
			}

			public String getSearchStringEscape()
			{
				return null;
			}

			public String getExtraNameCharacters()
			{
				return "";
			}

			public boolean supportsAlterTableWithAddColumn()
			{
				return false;
			}

			public boolean supportsAlterTableWithDropColumn()
			{
				return false;
			}

			public boolean supportsColumnAliasing()
			{
				return false;
			}

			public boolean nullPlusNonNullIsNull()
			{
				return false;
			}

			public boolean supportsConvert()
			{
				return false;
			}

			public boolean supportsConvert(int fromType, int toType)
			{
				return false;
			}

			public boolean supportsTableCorrelationNames()
			{
				return false;
			}

			public boolean supportsDifferentTableCorrelationNames()
			{
				return false;
			}

			public boolean supportsExpressionsInOrderBy()
			{
				return false;
			}

			public boolean supportsOrderByUnrelated()
			{
				return false;
			}

			public boolean supportsGroupBy()
			{
				return false;
			}

			public boolean supportsGroupByUnrelated()
			{
				return false;
			}

			public boolean supportsGroupByBeyondSelect()
			{
				return false;
			}

			public boolean supportsLikeEscapeClause()
			{
				return false;
			}

			public boolean supportsMultipleResultSets()
			{
				return false;
			}

			public boolean supportsMultipleTransactions()
			{
				return false;
			}

			public boolean supportsNonNullableColumns()
			{
				return false;
			}

			public boolean supportsMinimumSQLGrammar()
			{
				return false;
			}

			public boolean supportsCoreSQLGrammar()
			{
				return false;
			}

			public boolean supportsExtendedSQLGrammar()
			{
				return false;
			}

			public boolean supportsANSI92EntryLevelSQL()
			{
				return false;
			}

			public boolean supportsANSI92IntermediateSQL()
			{
				return false;
			}

			public boolean supportsANSI92FullSQL()
			{
				return false;
			}

			public boolean supportsIntegrityEnhancementFacility()
			{
				return false;
			}

			public boolean supportsOuterJoins()
			{
				return false;
			}

			public boolean supportsFullOuterJoins()
			{
				return false;
			}

			public boolean supportsLimitedOuterJoins()
			{
				return false;
			}

			public String getSchemaTerm()
			{
				return null;
			}

			public String getProcedureTerm()
			{
				return null;
			}

			public String getCatalogTerm()
			{
				return null;
			}

			public boolean isCatalogAtStart()
			{
				return false;
			}

			public String getCatalogSeparator()
			{
				return null;
			}

			public boolean supportsSchemasInDataManipulation()
			{
				return false;
			}

			public boolean supportsSchemasInProcedureCalls()
			{
				return false;
			}

			public boolean supportsSchemasInTableDefinitions()
			{
				return false;
			}

			public boolean supportsSchemasInIndexDefinitions()
			{
				return false;
			}

			public boolean supportsSchemasInPrivilegeDefinitions()
			{
				return false;
			}

			public boolean supportsCatalogsInDataManipulation()
			{
				return false;
			}

			public boolean supportsCatalogsInProcedureCalls()
			{
				return false;
			}

			public boolean supportsCatalogsInTableDefinitions()
			{
				return false;
			}

			public boolean supportsCatalogsInIndexDefinitions()
			{
				return false;
			}

			public boolean supportsCatalogsInPrivilegeDefinitions()
			{
				return false;
			}

			public boolean supportsPositionedDelete()
			{
				return false;
			}

			public boolean supportsPositionedUpdate()
			{
				return false;
			}

			public boolean supportsSelectForUpdate()
			{
				return false;
			}

			public boolean supportsStoredProcedures()
			{
				return false;
			}

			public boolean supportsSubqueriesInComparisons()
			{
				return false;
			}

			public boolean supportsSubqueriesInExists()
			{
				return false;
			}

			public boolean supportsSubqueriesInIns()
			{
				return false;
			}

			public boolean supportsSubqueriesInQuantifieds()
			{
				return false;
			}

			public boolean supportsCorrelatedSubqueries()
			{
				return false;
			}

			public boolean supportsUnion()
			{
				return false;
			}

			public boolean supportsUnionAll()
			{
				return false;
			}

			public boolean supportsOpenCursorsAcrossCommit()
			{
				return false;
			}

			public boolean supportsOpenCursorsAcrossRollback()
			{
				return false;
			}

			public boolean supportsOpenStatementsAcrossCommit()
			{
				return false;
			}

			public boolean supportsOpenStatementsAcrossRollback()
			{
				return false;
			}

			public int getMaxBinaryLiteralLength()
			{
				return 0;
			}

			public int getMaxCharLiteralLength()
			{
				return 0;
			}

			public int getMaxColumnNameLength()
			{
				return 0;
			}

			public int getMaxColumnsInGroupBy()
			{
				return 0;
			}

			public int getMaxColumnsInIndex()
			{
				return 0;
			}

			public int getMaxColumnsInOrderBy()
			{
				return 0;
			}

			public int getMaxColumnsInSelect()
			{
				return 0;
			}

			public int getMaxColumnsInTable()
			{
				return 0;
			}

			public int getMaxConnections()
			{
				return 0;
			}

			public int getMaxCursorNameLength()
			{
				return 0;
			}

			public int getMaxIndexLength()
			{
				return 0;
			}

			public int getMaxSchemaNameLength()
			{
				return 0;
			}

			public int getMaxProcedureNameLength()
			{
				return 0;
			}

			public int getMaxCatalogNameLength()
			{
				return 0;
			}

			public int getMaxRowSize()
			{
				return 0;
			}

			public boolean doesMaxRowSizeIncludeBlobs()
			{
				return false;
			}

			public int getMaxStatementLength()
			{
				return 0;
			}

			public int getMaxStatements()
			{
				return 0;
			}

			public int getMaxTableNameLength()
			{
				return 0;
			}

			public int getMaxTablesInSelect()
			{
				return 0;
			}

			public int getMaxUserNameLength()
			{
				return 0;
			}

			public int getDefaultTransactionIsolation()
			{
				return 0;
			}

			public boolean supportsTransactions()
			{
				return false;
			}

			public boolean supportsTransactionIsolationLevel(int level)
			{
				return false;
			}

			public boolean supportsDataDefinitionAndDataManipulationTransactions()
			{
				return false;
			}

			public boolean supportsDataManipulationTransactionsOnly()
			{
				return false;
			}

			public boolean dataDefinitionCausesTransactionCommit()
			{
				return false;
			}

			public boolean dataDefinitionIgnoredInTransactions()
			{
				return false;
			}

			public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			{
				return null;
			}

			public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
			{
				return null;
			}

			public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			{
				return null;
			}

			public ResultSet getSchemas()
			{
				return null;
			}

			public ResultSet getCatalogs()
			{
				return null;
			}

			public ResultSet getTableTypes()
			{
				return null;
			}

			public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			{
				return null;
			}

			public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			{
				return null;
			}

			public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			{
				return null;
			}

			public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			{
				return null;
			}

			public ResultSet getVersionColumns(String catalog, String schema, String table)
			{
				return null;
			}

			public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			{
				return null;
			}

			public ResultSet getImportedKeys(String catalog, String schema, String table)
			{
				return null;
			}

			public ResultSet getExportedKeys(String catalog, String schema, String table)
			{
				return null;
			}

			public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable)
			{
				return null;
			}

			public ResultSet getTypeInfo()
			{
				return null;
			}

			public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			{
				return null;
			}

			public boolean supportsResultSetType(int type)
			{
				return false;
			}

			public boolean supportsResultSetConcurrency(int type, int concurrency)
			{
				return false;
			}

			public boolean ownUpdatesAreVisible(int type)
			{
				return false;
			}

			public boolean ownDeletesAreVisible(int type)
			{
				return false;
			}

			public boolean ownInsertsAreVisible(int type)
			{
				return false;
			}

			public boolean othersUpdatesAreVisible(int type)
			{
				return false;
			}

			public boolean othersDeletesAreVisible(int type)
			{
				return false;
			}

			public boolean othersInsertsAreVisible(int type)
			{
				return false;
			}

			public boolean updatesAreDetected(int type)
			{
				return false;
			}

			public boolean deletesAreDetected(int type)
			{
				return false;
			}

			public boolean insertsAreDetected(int type)
			{
				return false;
			}

			public boolean supportsBatchUpdates()
			{
				return false;
			}

			public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			{
				return null;
			}

			public Connection getConnection()
			{
				return null;
			}

			public boolean supportsSavepoints()
			{
				return false;
			}

			public boolean supportsNamedParameters()
			{
				return false;
			}

			public boolean supportsMultipleOpenResults()
			{
				return false;
			}

			public boolean supportsGetGeneratedKeys()
			{
				return false;
			}

			public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
			{
				return null;
			}

			public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
			{
				return null;
			}

			public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
			{
				return null;
			}

			public boolean supportsResultSetHoldability(int holdability)
			{
				return false;
			}

			public int getResultSetHoldability()
			{
				return 0;
			}

			public int getDatabaseMajorVersion()
			{
				return 0;
			}

			public int getDatabaseMinorVersion()
			{
				return 0;
			}

			public int getJDBCMajorVersion()
			{
				return 0;
			}

			public int getJDBCMinorVersion()
			{
				return 0;
			}

			public int getSQLStateType()
			{
				return 0;
			}

			public boolean locatorsUpdateCopy()
			{
				return false;
			}

			public boolean supportsStatementPooling()
			{
				return false;
			}

			@Override
			public boolean autoCommitFailureClosesAllResultSets() throws SQLException
			{
				return false;
			}

			@Override
			public ResultSet getClientInfoProperties() throws SQLException
			{
				return null;
			}

			@Override
			public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException
			{
				return null;
			}

			@Override
			public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException
			{
				return null;
			}

			@Override
			public RowIdLifetime getRowIdLifetime() throws SQLException
			{
				return null;
			}

			@Override
			public ResultSet getSchemas(String arg0, String arg1) throws SQLException
			{
				return null;
			}

			@Override
			public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
			{
				return false;
			}

			@Override
			public boolean isWrapperFor(Class<?> arg0) throws SQLException
			{
				return false;
			}

			@Override
			public <T> T unwrap(Class<T> arg0) throws SQLException
			{
				return null;
			}
			
		};
	}

	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public void setReadOnly(boolean arg0)
	{
	}

	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly()
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(String arg0)
	{
	}

	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog()
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(int arg0)
	{
	}

	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation()
	{
		return 0;
	}

	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings()
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings()
	{
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public Statement createStatement(int arg0, int arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map<String, Class<?>> getTypeMap()
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(int arg0)
	{
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability()
	{
		return 0;
	}

	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	public Savepoint setSavepoint()
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public Savepoint setSavepoint(String arg0)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(Savepoint arg0)
	{
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(Savepoint arg0)
	{
	}

	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(int arg0, int arg1, int arg2)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public PreparedStatement prepareStatement(String arg0, int arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public PreparedStatement prepareStatement(String arg0, int[] arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	public PreparedStatement prepareStatement(String arg0, String[] arg1)
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createArrayOf(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createBlob()
	 */
	@Override
	public Blob createBlob() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	@Override
	public Clob createClob() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	@Override
	public NClob createNClob() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createSQLXML()
	 */
	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
	 */
	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#getClientInfo()
	 */
	@Override
	public Properties getClientInfo() throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#getClientInfo(java.lang.String)
	 */
	@Override
	public String getClientInfo(String arg0) throws SQLException
	{
		return null;
	}

	/**
	 * @see java.sql.Connection#isValid(int)
	 */
	@Override
	public boolean isValid(int arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.Connection#setClientInfo(java.util.Properties)
	 */
	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException
	{
	}

	/**
	 * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
	 */
	@Override
	public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException
	{
	}

	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
	{
	}

	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		return false;
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		return null;
	}
}
