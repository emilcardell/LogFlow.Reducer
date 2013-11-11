﻿using System;

namespace LogFlow.Reducer
{
	public class ReductionSettings
	{
		public ReductionSettings()
		{
			TimeInterval = TimeInterval.Day;
			Host = "localhost";
			Port = 9200;
			IndexNameFormat = @"\R\e\d\u\c\t\t\i\o\n\";
			ConnectionLimit = 5;
		}
		public TimeInterval TimeInterval { get; set; }
		public string Type { get; set; }
		public DateTime StartDate = DateTime.UtcNow.AddDays(-30);
		public string Host { get; set; }
		public int Port { get; set; }
		public string Ttl { get; set; }
		public int ConnectionLimit { get; set; }
		public string IndexNameFormat { get; set; }

	}
}