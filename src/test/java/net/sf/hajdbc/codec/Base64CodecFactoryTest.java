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
package net.sf.hajdbc.codec;

import java.sql.SQLException;

import net.sf.hajdbc.codec.base64.Base64CodecFactory;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class Base64CodecFactoryTest
{
	private CodecFactory factory = new Base64CodecFactory();
	
	@Test
	public void encode() throws SQLException
	{
		Codec codec = this.factory.createCodec(null);
		
		Assert.assertEquals("cGFzc3dvcmQ=", codec.encode("password"));
	}
	
	@Test
	public void decode() throws SQLException
	{
		Codec codec = this.factory.createCodec(null);
		
		Assert.assertEquals("password", codec.decode("cGFzc3dvcmQ="));
	}
}
