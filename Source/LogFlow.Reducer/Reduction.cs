
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace LogFlow.Reducer
{
	public abstract class Reduction<T> : IReduction
	{
		private readonly ElasticClient _client;
		private readonly RawElasticClient _rawClient;
		public protected Reduction(ElasticSearchConfiguration configuration)
		{
			_configuration = configuration;
			var clientSettings = new ConnectionSettings(new Uri(string.Format("http://{0}:{1}", configuration.Host, configuration.Port)));
			_rawClient = new RawElasticClient(clientSettings);
			_client = new ElasticClient(clientSettings);
		}

		protected ReductionStructure<T> CreateReductionWithSettings(Action<ReductionSettings> configureSettings)
		{
			return new ReductionStructure<T>(configureSettings);
		}

		protected ReductionStructure<T> CreateReduction()
		{
			return new ReductionStructure<T>();
		}

		public void Start()
		{
			// Load a unit full of data from Elastic Search




			var result = new Dictionary<string, JObjectAndHelper<T>>();
			Parallel.ForEach(new List<JObject>(), () => new Dictionary<string, JObjectAndHelper<T>>(),
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

			//Save result

			throw new NotImplementedException();
		}

		public void Stop()
		{
			throw new NotImplementedException();
		}

		public bool Validate()
		{
			return true;
		}
	}
}
