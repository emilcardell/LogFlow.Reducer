
using System;

namespace LogFlow.Reducer
{
	public abstract class Reduction<T>
	{
		private Action<ReductionSettings> settings;

		protected ReductionStructure<T> CreateReductionWithSettings(Action<ReductionSettings> settings)
		{
			this.settings = settings;
			return new ReductionStructure<T>();
		}

		protected ReductionStructure<T> CreateReduction()
		{
			return new ReductionStructure<T>();
		}
		
	}
}
