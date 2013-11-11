using System;
using System.IO;
using NLog;
using Topshelf;

namespace LogFlow.Reducer
{
	class Program
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		static void Main(string[] args)
		{
			try
			{
				Directory.SetCurrentDirectory(AppDomain.CurrentDomain.BaseDirectory);

				HostFactory.Run(x =>
				{
					x.Service<ReducerEngine>(s =>
					{
						s.ConstructUsing(name => new ReducerEngine());
						s.WhenStarted(tc => tc.Start());
						s.WhenStopped(tc => tc.Stop());
					});

					x.RunAsLocalSystem();
					x.SetDescription("Fluently reduces elasticsearch log entries in time series");
					x.SetDisplayName("LogFlow.MapReducer");
					x.SetServiceName("LogFlow.MapReducer");
				});
			}
			catch(Exception ex)
			{
				Log.Error(ex);
			}
		}
	}
}
