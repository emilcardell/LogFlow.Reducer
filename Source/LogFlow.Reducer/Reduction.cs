using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Nest;
using NLog;

namespace LogFlow.Reducer
{
	public abstract class Reduction<TIn, TOut, THelp> : IReduction where TIn : class  
	{
		private const int TimesToRetry = 10;
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();
		private ReductionStructure<TIn, TOut, THelp> _reductionStructure = new ReductionStructure<TIn, TOut, THelp>();
		private FlowStatus _currentStatus = FlowStatus.Stopped;
		
		protected ReductionStructure<TIn, TOut, THelp> CreateReductionWithSettings(Action<ReductionSettings> configureSettings)
		{
			_reductionStructure = new ReductionStructure<TIn, TOut, THelp>(configureSettings);
			return _reductionStructure;
		}

		protected ReductionStructure<TIn, TOut, THelp> CreateReduction()
		{
			_reductionStructure = new ReductionStructure<TIn, TOut, THelp>();
			return _reductionStructure;
		}

		private ElasticClient _client;

		private CancellationTokenSource _tokenSource;
		private Task _processTask;
		
		public void Start()
		{
			Log.Info(string.Format("{0}: Starting.", GetType().Name));
			_tokenSource = new CancellationTokenSource();
			_processTask = Task.Factory.StartNew(ExecuteProcess, _tokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
		}

		private void ExecuteProcess()
		{
			var retriedTimes = 0;

			_currentStatus = FlowStatus.Running;
			Log.Info(string.Format("{0}: Started.", GetType().Name));

			while (true)
			{
				try
				{
					_tokenSource.Token.ThrowIfCancellationRequested();
					ExecutePeriod();
				}
				catch (OperationCanceledException)
				{
					_currentStatus = FlowStatus.Stopped;
					Log.Info(string.Format("{0}: Stopped.", GetType().Name));
					break;
				}
				catch (Exception ex)
				{
					if (retriedTimes < TimesToRetry)
					{
						retriedTimes++;
						_currentStatus = FlowStatus.Retrying;

						Log.Warn(string.Format("{0}: {1}", GetType().Name, ex));
						Log.Warn(string.Format("{0}: Retrying {1} times.", GetType().Name, retriedTimes));
						Thread.Sleep(TimeSpan.FromSeconds(10));
						_tokenSource.Token.ThrowIfCancellationRequested();
						continue;
					}

					_currentStatus = FlowStatus.Broken;

					Log.Error(ex);
					Log.Error(string.Format("{0}: Shut down because broken!", GetType().Name));
					break;
				}

				if (_currentStatus == FlowStatus.Retrying)
				{
					_currentStatus = FlowStatus.Running;

					Log.Info(string.Format("{0}: Resuming after {1} times.", GetType().Name, retriedTimes));
					retriedTimes = 0;
				}
			}
		}

		private const int NumberOfDocumentsPerRead = 10000;
		private void ExecutePeriod()
		{
			var clientSettings =
				new ConnectionSettings(
					new Uri(string.Format("http://{0}:{1}", _reductionStructure.Settings.Host,
										  _reductionStructure.Settings.Port)));
			clientSettings.SetDefaultIndex("_all");
			_client = new ElasticClient(clientSettings);
			

			ReductionPeriod period = LoadPeriod(_reductionStructure.Settings);

			Log.Trace("Search type:" + typeof(TIn).Name);
			var totalLogLines = new List<TIn>();
			var position = 0;
			while (true)
			{
				int currentPosition = position;
				var logLines = _client.Search<TIn>(
				s => s.Skip(currentPosition).Take(NumberOfDocumentsPerRead).Query(q =>
											  q.Range(r =>
													  r.OnField(ElasticSearchFields.Timestamp)
													   .From(period.From)
													   .To(period.To)
													   .ToExclusive()
												  )));
				totalLogLines.AddRange(logLines.Documents);

				if (logLines.Total == totalLogLines.Count)
					break;

				position += logLines.Documents.Count();
			}

			Log.Trace(totalLogLines.Count() + " log entries found between" + period.From.ToString("yyyy-MM-dd") + " and " + period.To.ToString("yyyy-MM-dd"));

			var result = new Dictionary<string, ReductionResultData<TOut, THelp>>();

			Parallel.ForEach(totalLogLines, () => new Dictionary<string, ReductionResultData<TOut, THelp>>(),
				(record, loopControl, localDictionary) =>
				{
					_tokenSource.Token.ThrowIfCancellationRequested();
					return _reductionStructure.Reducer(record, localDictionary);
				}
							 ,
							 (localDictionary) =>
								 {
									 _tokenSource.Token.ThrowIfCancellationRequested();
									 lock (result)
									 {
										 _reductionStructure.Combiner(result, localDictionary);
									 }
								 });
			
			EnsureIndexExists(_reductionStructure.Settings.IndexName);

			
			foreach (var reductionResultData in result)
			{
				_client.Index(reductionResultData.Value, _indexName, GetType().Name, reductionResultData.Key);
				_client.Flush();
			}


			if(period.IsCurrent)
			{
				Thread.Sleep(TimeSpan.FromSeconds(60));
				return;
			}
				
			SetNewPeriod(_reductionStructure.Settings, period);



		}

		private void SetNewPeriod(ReductionSettings settings, ReductionPeriod reductionPeriod)
		{
			var storageKey = GetPositionStorageKey(settings);
			_storage.Insert(storageKey, reductionPeriod.To);
		}

		private readonly StateStorage _storage = new StateStorage("PeriodStateStorageKey");
		private ReductionPeriod LoadPeriod(ReductionSettings settings)
		{
			var storageKey = GetPositionStorageKey(settings);
			DateTime? startDate;

			try
			{
				startDate = _storage.Get<DateTime?>(storageKey);
				Log.Trace("Loaded start date from storage: " + startDate);
			}
			catch (Exception)
			{
				startDate = settings.StartDate;
			}

			if(startDate == null)
				startDate =  DateTime.UtcNow.AddDays(-30);

			var result = new ReductionPeriod();

			result.From = new DateTime(startDate.Value.Year, startDate.Value.Month, startDate.Value.Day, 0, 0, 0, 0);
			result.To = result.From.AddDays(1);
			result.IsCurrent = startDate.Value.Date == DateTime.UtcNow.Date;

			return result;
		}

		private string GetPositionStorageKey(ReductionSettings settings)
		{
			return "poistion_" + GetType().Name + "_" + settings.TimeInterval;
		}

		public void Stop()
		{
			if (_tokenSource == null) return;

			Log.Info(string.Format("{0}: Stopping.", GetType().Name));
			_tokenSource.Cancel();

			if (_processTask != null)
			{
				_processTask.Wait();
			}
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
			indexName = indexName.ToLowerInvariant();

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
