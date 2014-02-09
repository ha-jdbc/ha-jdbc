/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.tx;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple transaction identifier factory using an incrementing counter.
 * This implementation is *not* safe for <distributable/> clusters, since the identifiers are only unique within a single DatabaseCluster instance.
 * @author Paul Ferraro
 */
public class SimpleTransactionIdentifierFactory implements TransactionIdentifierFactory<Long>
{
	private final AtomicLong counter = new AtomicLong(0);
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#createTransactionIdentifier()
	 */
	@Override
	public Long createTransactionIdentifier()
	{
		return this.counter.incrementAndGet();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(Long transactionId)
	{
		return ByteBuffer.allocate(this.size()).putLong(transactionId).array();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#deserialize(byte[])
	 */
	@Override
	public Long deserialize(byte[] bytes)
	{
		return ByteBuffer.wrap(bytes).getLong();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#size()
	 */
	@Override
	public int size()
	{
		return Long.SIZE;
	}
}