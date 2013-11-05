
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
			IndexNameFormat = @"\l\o\g\f\l\o\w\-yyyyMM";
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
	
	public class ReductionStructure<T>
	{
		private ReductionSettings Settings;
		public Func<ReductionSettings> SetSettings { get; set; }
		public Func<JObject, Dictionary<string, JObjectAndHelper<T>>, Dictionary<string, JObjectAndHelper<T>>> Reducer { get; set; }
		public Func<Dictionary<string, JObjectAndHelper<T>>, Dictionary<string, JObjectAndHelper<T>>> Combiner { get; set; }

		public ReductionStructure<T> ReduceIntputWith(Func<JObject, Dictionary<string, JObjectAndHelper<T>>, Dictionary<string, JObjectAndHelper<T>>> reducer)
		{
			Reducer = reducer;
			return this;
		}

		public void CombineResultInto(Func<Dictionary<string, JObjectAndHelper<T>>, Dictionary<string, JObjectAndHelper<T>>> combiner)
		{
			Combiner = combiner;
		}

		public void Start()
		{
			if(SetSettings != null)
				Settings = SetSettings();

			var result = new Dictionary<string, JObject>();
			Parallel.ForEach(new List<JObject>(), () => { return new Dictionary<string, JObject>(); },
				(record, loopControl, localDictionary) =>
				{
					return localDictionary;
				},
				(localDictionary) =>
				{
					lock(result)
					{

					}
				});

		}
	}
}
