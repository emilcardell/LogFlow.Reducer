namespace LogFlow.Reducer
{
	public interface IReduction
	{
		void Start();
		void Stop();
		bool Validate();
	}
}