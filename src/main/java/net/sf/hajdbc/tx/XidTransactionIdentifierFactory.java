/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
import java.util.Random;

import javax.transaction.xa.Xid;

/**
 * Factory for generating transaction identifiers.
 * Only the serialization methods are used during runtime, as {@link Xid}s are determined by the underlying XAResource.
 * @author Paul Ferraro
 */
public class XidTransactionIdentifierFactory implements TransactionIdentifierFactory<Xid>
{
	/**
	 * {@inheritDoc}
	 * Only used for testing purposes.
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#createTransactionIdentifier()
	 */
	@Override
	public Xid createTransactionIdentifier()
	{
		Random random = new Random(System.currentTimeMillis());
		
		final int formatId = random.nextInt();
		final byte[] globalTransactionId = new byte[Xid.MAXGTRIDSIZE];
		random.nextBytes(globalTransactionId);
		final byte[] branchQualifier = new byte[Xid.MAXBQUALSIZE];
		random.nextBytes(branchQualifier);
		
		return new Xid()
		{
			@Override
			public int getFormatId()
			{
				return formatId;
			}

			@Override
			public byte[] getGlobalTransactionId()
			{
				return globalTransactionId;
			}

			@Override
			public byte[] getBranchQualifier()
			{
				return branchQualifier;
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(Xid xid)
	{
		return ByteBuffer.allocate(this.size()).putInt(xid.getFormatId()).put(xid.getGlobalTransactionId()).put(xid.getBranchQualifier()).array();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#deserialize(byte[])
	 */
	@Override
	public Xid deserialize(byte[] bytes)
	{
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		final int formatId = buffer.getInt();
		final byte[] globalTransactionId = new byte[Xid.MAXGTRIDSIZE];
		buffer.get(globalTransactionId);
		final byte[] branchQualifier = new byte[Xid.MAXBQUALSIZE];
		buffer.get(branchQualifier);
		
		return new Xid()
		{
			@Override
			public int getFormatId()
			{
				return formatId;
			}

			@Override
			public byte[] getGlobalTransactionId()
			{
				return globalTransactionId;
			}

			@Override
			public byte[] getBranchQualifier()
			{
				return branchQualifier;
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.tx.TransactionIdentifierFactory#size()
	 */
	@Override
	public int size()
	{
		return Integer.SIZE + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE;
	}
}
