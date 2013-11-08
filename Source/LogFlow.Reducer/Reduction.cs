using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Nest;
using Newtonsoft.Json.Linq;
using NLog;

namespace LogFlow.Reducer
{
	public abstract class Reduction<TIn, TOut, THelp> : IReduction where TIn : class  
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();
		private static ReductionStructure<TIn, TOut, THelp> reductionStructure = new ReductionStructure<TIn, TOut, THelp>();
		
		protected ReductionStructure<TIn, TOut, THelp> CreateReductionWithSettings(Action<ReductionSettings> configureSettings)
		{
			return new ReductionStructure<TIn, TOut, THelp>(configureSettings);
		}

		protected ReductionStructure<TIn, TOut, THelp> CreateReduction()
		{
			return new ReductionStructure<TIn, TOut, THelp>();
		}

		private ElasticClient _client;
		private RawElasticClient _rawClient;
		
		public void Start()
		{
			// Load a unit full of data from Elastic Search
			var clientSettings = new ConnectionSettings(new Uri(string.Format("http://{0}:{1}", Settings.Host, configuration.Port)));
			_rawClient = new RawElasticClient(clientSettings);
			_client = new ElasticClient(clientSettings);

			var blahonga = _client.Search<TIn> (
										s => s.Skip(0).Take(10).Query(q =>
																 q.Range(r =>
																				 r.OnField(ElasticSearchFields.Timestamp)
																				  .From(dateToAggregate)
																				  .To(dateToAggregate.AddDays(1))
																				  .ToExclusive()
																		 )));
			_rawClient.SearchPost()



			var result = new Dictionary<string, ReductionData<T>>();
			Parallel.ForEach(new List<JObject>(), () => new Dictionary<string, ReductionData<T>>(),
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

		private string _indexName = string.Empty;
		private void EnsureIndexExists(string indexName)
		{
			if(!string.IsNullOrWhiteSpace(_indexName))
				return;

			CreateIndex(indexName);
			_indexName = indexName;
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

			Log.Trace(string.Format("{0}: Index '{1}' i successfully created.", GetType().Name, indexName));
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
					.Date(m => m.Name(ElasticSearchFields.Timestamp).Index(NonStringIndexOption.not_analyzed))
					.String(m => m.Name(ElasticSearchFields.Type).Index(FieldIndexOption.not_analyzed))
				)
			);
		}
	}
}
