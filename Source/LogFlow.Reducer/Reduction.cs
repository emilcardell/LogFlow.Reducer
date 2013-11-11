using System;
using System.Collections.Generic;
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
                    ExecutePeriod();
                    _tokenSource.Token.ThrowIfCancellationRequested();
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

	    private void ExecutePeriod()
	    {
	        var clientSettings =
	            new ConnectionSettings(
	                new Uri(string.Format("http://{0}:{1}", _reductionStructure.Settings.Host,
	                                      _reductionStructure.Settings.Port)));
	        _client = new ElasticClient(clientSettings);

	        ReductionPeriod period = LoadPeriod(_reductionStructure.Settings);

	        var logLines = _client.Search<TIn>(
	            s => s.Skip(0).Take(int.MaxValue).Query(q =>
	                                          q.Range(r =>
	                                                  r.OnField(ElasticSearchFields.Timestamp)
	                                                   .From(period.From)
	                                                   .To(period.To)
	                                                   .ToExclusive()
	                                              )));


	        var result = new Dictionary<string, ReductionResultData<TOut, THelp>>();

	        Parallel.ForEach(logLines.Documents, () => new Dictionary<string, ReductionResultData<TOut, THelp>>(),
	                         (record, loopControl, localDictionary) =>
	                         _reductionStructure.Reducer(record, localDictionary),
	                         (localDictionary) =>
	                             {
	                                 lock (result)
	                                 {
	                                     _reductionStructure.Combiner(result, localDictionary);
	                                 }
	                             });
            
            EnsureIndexExists(_reductionStructure.Settings.IndexName);

	        foreach (var reductionResultData in result)
	        {
	            _client.Index(reductionResultData.Value, _indexName, GetType().Name, reductionResultData.Key);
	        }
	        _client.Flush();

	        SetNewPeriod(_reductionStructure.Settings, period);

	        if (period.IsCurrent)
	            Thread.Sleep(TimeSpan.FromSeconds(60));

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
		    var startDate = DateTime.UtcNow.AddDays(-30);

		    try
		    {
		        startDate = _storage.Get<DateTime>(storageKey);
		    }
		    catch (Exception)
		    {
		        startDate = settings.StartDate;
		    }

		    var result = new ReductionPeriod();

		    result.From = new DateTime(startDate.Year, startDate.Month, startDate.Day, 0, 0, 0, 0);
		    result.To = result.From.AddDays(1);
		    result.IsCurrent = startDate.Date == DateTime.UtcNow.Date;

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
