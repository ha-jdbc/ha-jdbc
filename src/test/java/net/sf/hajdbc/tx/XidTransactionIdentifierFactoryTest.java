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

import java.util.Arrays;

import javax.transaction.xa.Xid;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class XidTransactionIdentifierFactoryTest
{
	private TransactionIdentifierFactory<Xid> factory = new XidTransactionIdentifierFactory();
	
	@Test
	public void test()
	{
		Xid expected = this.factory.createTransactionIdentifier();
		
		byte[] bytes = this.factory.serialize(expected);
		
		Assert.assertEquals(this.factory.size(), bytes.length);
		
		Xid result = this.factory.deserialize(bytes);
		
		Assert.assertEquals(expected.getFormatId(), result.getFormatId());
		Assert.assertTrue(Arrays.equals(expected.getGlobalTransactionId(), result.getGlobalTransactionId()));
		Assert.assertTrue(Arrays.equals(expected.getBranchQualifier(), result.getBranchQualifier()));
	}
}
