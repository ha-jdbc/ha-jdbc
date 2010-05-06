package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;

public class PreparedStatementInvocationHandlerFactory<Z, D extends Database<Z>> extends AbstractStatementInvocationHandlerFactory<Z, D, PreparedStatement>
{
	private final String sql;

	public PreparedStatementInvocationHandlerFactory(TransactionContext<Z, D> context, String sql)
	{
		super(PreparedStatement.class, context);

		this.sql = sql;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandlerFactory#createInvocationHandler(java.sql.Connection, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker, java.util.Map, net.sf.hajdbc.sql.TransactionContext, net.sf.hajdbc.sql.FileSupport)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, PreparedStatement, SQLException> invoker, Map<D, PreparedStatement> statements, TransactionContext<Z, D> context, FileSupport<SQLException> fileSupport) throws SQLException
	{
		return new PreparedStatementInvocationHandler<Z, D>(connection, proxy, invoker, statements, context, fileSupport, this.sql);
	}
}
