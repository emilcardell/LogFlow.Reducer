
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using Newtonsoft.Json.Linq;

namespace LogFlow.Reducer
{
	public abstract class Reduction<T> : IReduction
	{
		

		protected ReductionStructure<T> CreateReductionWithSettings(Action<ReductionSettings> configureSettings)
		{
			return new ReductionStructure<T>(configureSettings);
		}

		protected ReductionStructure<T> CreateReduction()
		{
			return new ReductionStructure<T>();
		}

		private ElasticClient _client;
		private RawElasticClient _rawClient;
		
		public void Start()
		{
			// Load a unit full of data from Elastic Search
			var clientSettings = new ConnectionSettings(new Uri(string.Format("http://{0}:{1}", configuration.Host, configuration.Port)));
			_rawClient = new RawElasticClient(clientSettings);
			_client = new ElasticClient(clientSettings);

			var blahonga = _client.Search (
										s => s.Skip(0).Take(10).Query(q =>
																 q.Range(r =>
																				 r.OnField(ElasticSearchFields.Timestamp)
																				  .From(dateToAggregate)
																				  .To(dateToAggregate.AddDays(1))
																				  .ToExclusive()
																		 )));
			_rawClient.SearchPost()



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

		private void EnsureIndexExists(string indexName)
		{
			if(_indexNames.Contains(indexName))
				return;

			CreateIndex(indexName);
			_indexNames.Add(indexName);
		}


		private void CreateIndex(string indexName)
		{
			if(_client.IndexExists(indexName).Exists)
				return;

			var indexSettings = new IndexSettings
				{
					{"index.store.compress.stored", true},
					{"index.store.compress.tv", true},
					{"index.query.default_field", ElasticSearchFields.Message}
				};

			IIndicesOperationResponse result = _client.CreateIndex(indexName, indexSettings);

			CreateMappings(indexName);

			if(!result.OK)
			{
				throw new ApplicationException(string.Format("Failed to create index: '{0}'. Result: '{1}'", indexName, result.ConnectionStatus.Result));
			}

			Log.Trace(string.Format("{0}: Index '{1}' i successfully created.", LogContext.LogType, indexName));
		}

		private void CreateMappings(string indexName)
		{
			_client.MapFluent(map => map
				.IndexName(indexName)
				.DisableAllField()
				.TypeName("_default_")
				.TtlField(t => t.SetDisabled(false))
				.SourceField(s => s.SetCompression())
				.Properties(descriptor => descriptor
					.String(m => m.Name(ElasticSearchFields.Source).Index(FieldIndexOption.not_analyzed))
					.Date(m => m.Name(ElasticSearchFields.Timestamp).Index(NonStringIndexOption.not_analyzed))
					.String(m => m.Name(ElasticSearchFields.Type).Index(FieldIndexOption.not_analyzed))
					.String(m => m.Name(ElasticSearchFields.Message).IndexAnalyzer("whitespace"))
				)
			);
		}
	}
}
