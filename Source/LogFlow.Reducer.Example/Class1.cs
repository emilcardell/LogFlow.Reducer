
using System.Collections.Generic;

namespace LogFlow.Reducer.Example
{
	public class Class1 : Reduction<List<string>>
	{
		public Class1()
		{
			CreateReductionWithSettings(settings =>
			{
				settings.Host = "";
			}).ReduceIntputWith((inputData, result) =>
			{
				var something = result["lasse"];
				if(something.Helper.Exists("Nisse"))

				return result;
			});
		}
	}

	public class Reducer
	{
		
	}
}
