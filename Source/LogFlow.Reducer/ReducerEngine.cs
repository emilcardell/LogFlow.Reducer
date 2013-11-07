using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using NLog;

namespace LogFlow.Reducer
{
	public class ReducerEngine
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();
		private static ReductionBuilder reductionBuilder = new ReductionBuilder();

		public bool Start()
		{
			Log.Trace("Starting");

			var path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

			Log.Trace("Assembly Path:" + path);

			var allAssemblies = Directory.GetFiles(path, "*.dll").Select(Assembly.LoadFile).ToList();

			var reductionTypes = allAssemblies
				.SelectMany(assembly => assembly.GetTypes())
				.Where(type => !type.IsAbstract)
				.Where(type => type.GetInterfaces().Contains(typeof(IReduction)));

			Log.Trace("Number of reductions found: " + reductionTypes.Count());

			foreach(var reductionType in reductionTypes)
			{
				try
				{
					var reduction = (IReduction)Activator.CreateInstance(reductionType);
					reductionBuilder.BuildAndRegisterReduction(reduction);
				}
				catch(Exception exception)
				{
					Log.Error(exception);
				}
			}

			Task.WaitAll(reductionBuilder.Reductions.Select(x => Task.Run(() => x.Start())).ToArray());

			//if(Config.EnableNancyHealthModule)
			//{
			//	Log.Info("Starting Nancy health module");
			//	_nancyHost = new NancyHost(new Uri(Config.NancyHostUrl));
			//	_nancyHost.Start();
			//	Log.Info("Started Nancy health module on " + Config.NancyHostUrl);
			//}


			return true;
		}

		public bool Stop()
		{
			return true;
		}

	}
}
