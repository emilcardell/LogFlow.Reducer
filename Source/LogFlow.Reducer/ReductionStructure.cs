
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

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

	public class ReductionStructure<TIn, TOut, THelp>
	{
		private readonly ReductionSettings Settings = new ReductionSettings();

		public ReductionStructure()
		{
		}

		public ReductionStructure(Action<ReductionSettings> configureSettings)
		{
			configureSettings(Settings);
		}

		public Func<JObject, Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> Reducer { get; set; }
		public Func<Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> Combiner { get; set; }

		public ReductionStructure<TIn, TOut, THelp> ReduceIntputWith(Func<JObject, Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> reducer)
		{
			Reducer = reducer;
			return this;
		}

		public void CombineResultInto(Func<Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> combiner)
		{
			Combiner = combiner;
		}
	}
}
