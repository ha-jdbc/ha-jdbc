/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
import java.util.UUID;

/**
 * Transaction identifier factory that generates random UUIDs.
 * This implementation is safe for <distributable/> clusters.
 * @author Paul Ferraro
 */
public class UUIDTransactionIdentifierFactory implements TransactionIdentifierFactory<UUID>
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#createTransactionIdentifier()
	 */
	@Override
	public UUID createTransactionIdentifier()
	{
		return UUID.randomUUID();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(UUID transactionId)
	{
		return ByteBuffer.allocate(this.size()).putLong(transactionId.getMostSignificantBits()).putLong(transactionId.getLeastSignificantBits()).array();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#deserialize(byte[])
	 */
	@Override
	public UUID deserialize(byte[] bytes)
	{
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		return new UUID(buffer.getLong(), buffer.getLong());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#size()
	 */
	@Override
	public int size()
	{
		return Long.SIZE * 2;
	}
}
