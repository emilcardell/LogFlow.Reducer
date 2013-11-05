using Newtonsoft.Json.Linq;

namespace LogFlow.Reducer
{
	public class JObjectAndHelper<T>
	{
		public JObject JObject { get; set; }
		public T Helper { get; set; }
	}
}