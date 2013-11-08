namespace LogFlow.Reducer
{
	public class ReductionResultData<TOut, THelp>
	{
		public TOut Output { get; set; }
		public THelp Helper { get; set; }
	}
}