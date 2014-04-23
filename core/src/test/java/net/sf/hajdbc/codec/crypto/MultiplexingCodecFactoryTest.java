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
package net.sf.hajdbc.codec.crypto;

import java.sql.SQLException;

import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.codec.DecoderFactory;
import net.sf.hajdbc.codec.MultiplexingDecoderFactory;

import org.junit.Assert;
import org.junit.Test;

public class MultiplexingCodecFactoryTest extends CipherCodecFactoryTest
{
	private final DecoderFactory factory = new MultiplexingDecoderFactory();
	
	@Test
	public void decodeSimple() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("password"));
	}
	
	@Test
	public void decodeHex() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("16:70617373776f7264"));
	}
	
	@Test
	public void decodeBase64() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("64:cGFzc3dvcmQ="));
	}
	
	@Test
	@Override
	public void test() throws SQLException
	{
		Decoder codec = this.factory.createDecoder(null);
		
		Assert.assertEquals("password", codec.decode("?:wzAkF0hlYUeGhfzRQIxYAQ=="));
	}
}
