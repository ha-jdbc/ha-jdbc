package net.sf.hajdbc.util.concurrent.cron;

import javax.xml.bind.annotation.adapters.XmlAdapter;


public class CronExpressionAdapter extends XmlAdapter<String, CronExpression>
{
	@Override
	public String marshal(CronExpression expression) throws Exception
	{
		return (expression != null) ? expression.getCronExpression() : null;
	}

	@Override
	public CronExpression unmarshal(String expression) throws Exception
	{
		return new CronExpression(expression);
	}
}
