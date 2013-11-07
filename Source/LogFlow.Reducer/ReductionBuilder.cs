using System.Collections.Generic;

namespace LogFlow.Reducer
{
	public class ReductionBuilder
	{
		public List<IReduction> Reductions = new List<IReduction>();

		public void BuildAndRegisterReduction(IReduction reduction)
		{
			if(reduction.Validate())
				Reductions.Add(reduction);
		}
	}
}
