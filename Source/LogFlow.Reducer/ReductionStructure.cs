
using System;
using System.Collections.Generic;

namespace LogFlow.Reducer
{
	public class ReductionStructure<TIn, TOut, THelp>
	{
		private readonly ReductionSettings _settings = new ReductionSettings();
		public ReductionSettings Settings { get { return _settings; } }

		public ReductionStructure()
		{
		}

		public ReductionStructure(Action<ReductionSettings> configureSettings)
		{
			configureSettings(_settings);
		}

		public Func<TIn, Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> Reducer { get; set; }
		public Action<Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> Combiner { get; set; }

		public ReductionStructure<TIn, TOut, THelp> ReduceIntputWith(Func<TIn, Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> reducer)
		{
			Reducer = reducer;
			return this;
		}

		public void CombineResultInto(Action<Dictionary<string, ReductionResultData<TOut, THelp>>, Dictionary<string, ReductionResultData<TOut, THelp>>> combiner)
		{
			Combiner = combiner;
		}
	}
}
