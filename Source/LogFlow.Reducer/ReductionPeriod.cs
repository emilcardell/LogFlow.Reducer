using System;

namespace LogFlow.Reducer
{
	public class ReductionPeriod
	{
		public DateTime From { get; set; }
		public DateTime To { get; set; }
		public bool IsCurrent { get; set; }
	}
}