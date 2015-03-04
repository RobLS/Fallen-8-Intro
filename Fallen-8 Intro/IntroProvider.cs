using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Collections.ObjectModel;
using System.Linq;
using NoSQL.GraphDB;
using NoSQL.GraphDB.Helper;
using NoSQL.GraphDB.Model;

namespace Intro
{
	public class IntroProvider
	{
		private List<VertexModel> _toBeBenchenVertices = null;
		private int _numberOfToBeTestedVertices = 10000000;
		private Fallen8 _f8;

		public IntroProvider (Fallen8 fallen8)
		{
			_f8 = fallen8;
		}

		/// <summary>
		/// Creates a scale free network
		/// </summary>
		/// <param name="nodeCound"></param>
		/// <param name="edgeCount"></param>
		/// <param name="fallen8"></param>
		public void CreateScaleFreeNetwork (int nodeCound, int edgeCount)
		{
			var creationDate = DateHelper.ConvertDateTime (DateTime.Now);
			var vertexIDs = new List<Int32> ();
			var prng = new Random ();
			if (nodeCound < _numberOfToBeTestedVertices) {
				_numberOfToBeTestedVertices = nodeCound;
			}

			_toBeBenchenVertices = new List<VertexModel> (_numberOfToBeTestedVertices);

			for (var i = 0; i < nodeCound; i++) {
                vertexIDs.Add(
                                    _f8.CreateVertex(creationDate, new PropertyContainer[1]
                                                           {
                                                               new PropertyContainer { PropertyId = 1, Value = ("Usuario"+i) }
                                                           }).Id);
                //vertexIDs.Add (_f8.CreateVertex (creationDate).Id);
                        
			}

			if (edgeCount != 0) {
				foreach (var aVertexId in vertexIDs) {
					var targetVertices = new HashSet<Int32> ();

					do {
						targetVertices.Add (vertexIDs [prng.Next (0, vertexIDs.Count)]);
					} while (targetVertices.Count < edgeCount);

					foreach (var aTargetVertex in targetVertices) {
                        //_f8.CreateEdge (aVertexId, 0, aTargetVertex, creationDate);
                        if (5 > prng.Next(0, 10))
                        {
                            _f8.CreateEdge(aVertexId, 0, aTargetVertex, creationDate, new PropertyContainer[1]
                                                           {
                                                               new PropertyContainer { PropertyId = 1001, Value = "Sigue A" }
                                                           });
                        }
                        else
                        {
                            _f8.CreateEdge(aVertexId, 0, aTargetVertex, creationDate, new PropertyContainer[1]
                                                           {
                                                               new PropertyContainer { PropertyId = 1002, Value = "Stalkea A" }
                                                           });
                        }
					}
				}

				_toBeBenchenVertices.AddRange (PickInterestingIDs (vertexIDs, prng)
				.Select (aId => {
					VertexModel v = null;

					_f8.TryGetVertex (out v, aId);

					return v;
				}));
			}
		}

		IEnumerable<int> PickInterestingIDs (List<int> vertexIDs, Random prng)
		{
			for (int i = 0; i < _numberOfToBeTestedVertices; i++) {
				yield return vertexIDs [prng.Next (0, vertexIDs.Count)];
			}
		}

		/// <summary>
		/// Benchmark
		/// </summary>
		/// <param name="fallen8"></param>
		/// <param name="myIterations"></param>
		/// <returns></returns>
		public String Bench (int myIterations = 1000)
		{
			if (_toBeBenchenVertices == null) {
				return "No vertices available";
			}

			List<VertexModel> vertices = _toBeBenchenVertices;
			var tps = new List<double> ();
			long edgeCount = 0;
			var sb = new StringBuilder ();
			
			Int32 range = ((vertices.Count / Environment.ProcessorCount) * 3) / 2;
			
			for (var i = 0; i < myIterations; i++) {
				var sw = Stopwatch.StartNew ();

				edgeCount = CountAllEdgesParallelPartitioner (vertices, range);

				sw.Stop ();

				tps.Add (edgeCount / sw.Elapsed.TotalSeconds);
			}

			sb.AppendLine (String.Format ("Traversed {0} edges. Average: {1}TPS Median: {2}TPS StandardDeviation {3}TPS ", edgeCount, Statistics.Average (tps), Statistics.Median (tps), Statistics.StandardDeviation (tps)));

			return sb.ToString ();
		}

		/// <summary>
		/// Counter
		/// </summary>
		/// <param name="vertices"></param>
		/// <param name="vertexRange"></param>
		/// <returns></returns>
		private static long CountAllEdgesParallelPartitioner (List<VertexModel> vertices, Int32 vertexRange)
		{
			var lockObject = new object ();
			var edgeCount = 0L;
			var rangePartitioner = Partitioner.Create (0, vertices.Count, vertexRange);

			Parallel.ForEach (
				rangePartitioner,
				() => 0L,
				delegate(Tuple<int, int> range, ParallelLoopState loopstate, long initialValue) {
					var localCount = initialValue;

					for (var i = range.Item1; i < range.Item2; i++) {
						ReadOnlyCollection<EdgeModel> outEdge;
						if (vertices [i].TryGetOutEdge (out outEdge, 0)) {
							for (int j = 0; j < outEdge.Count; j++) {
								var vertex = outEdge [j].TargetVertex;
								localCount++;
							}
						}
					}

					return localCount;
				},
				delegate(long localSum) {
					lock (lockObject) {
						edgeCount += localSum;
					}
				});

			return edgeCount;
		}

        public string GetVertexElementById(int id)
        {
            VertexModel element;

            _f8.TryGetVertex(out element, id);

            var sb = new StringBuilder();
            string userName;
            element.TryGetProperty<String>(out userName, 1);

            sb.AppendLine(String.Format("Elemento Id {0}. Element Creation Date {1}, Nombre de Usuario {2}", element.Id, element.CreationDate, userName));

            sb.AppendLine(String.Format("Sus Vecinos son: "));
            List<VertexModel> _neighbors = element.GetAllNeighbors();
            foreach (var _neighbor in _neighbors)
            {
                string targetUserName;
                _neighbor.TryGetProperty<String>(out targetUserName, 1);
                sb.AppendLine(String.Format(targetUserName));
            }

            ReadOnlyCollection<EdgeModel> _outEdges;
            sb.AppendLine(String.Format("Sigue A: "));
            element.TryGetOutEdge(out _outEdges, 0);
            foreach (var outEdge in _outEdges)
            {
                ReadOnlyCollection<PropertyContainer> _propertys = outEdge.GetAllProperties();
                foreach (var prop in _propertys)
                {
                    if (prop.PropertyId == 1001)
                    {
                        string targetUserName;
                        outEdge.TargetVertex.TryGetProperty<String>(out targetUserName, 1);
                        sb.AppendLine(String.Format(targetUserName));
                    }
                }
            }

            sb.AppendLine(String.Format("Stalkea A: "));
            foreach (var outEdge in _outEdges)
            {
                ReadOnlyCollection<PropertyContainer> _propertys = outEdge.GetAllProperties();
                foreach (var prop in _propertys)
                {
                    if (prop.PropertyId == 1002)
                    {
                        string targetUserName;
                        outEdge.TargetVertex.TryGetProperty<String>(out targetUserName, 1);
                        sb.AppendLine(String.Format(targetUserName));
                    }
                }
            }

            ReadOnlyCollection<EdgeModel> _inEdges;
            element.TryGetInEdge(out _inEdges, 0);

            sb.AppendLine(String.Format("Es  Seguido Por: "));
            foreach (var inEdge in _inEdges)
            {
                ReadOnlyCollection<PropertyContainer> _propertys = inEdge.GetAllProperties();
                foreach (var prop in _propertys)
                {
                    if (prop.PropertyId == 1001)
                    {
                        string targetUserName;
                        inEdge.SourceVertex.TryGetProperty<String>(out targetUserName, 1);
                        sb.AppendLine(String.Format(targetUserName));
                    }
                }
            }

            sb.AppendLine(String.Format("Es Stalkeado Por: "));
            foreach (var inEdge in _inEdges)
            {
                ReadOnlyCollection<PropertyContainer> _propertys = inEdge.GetAllProperties();
                foreach (var prop in _propertys)
                {
                    if (prop.PropertyId == 1002)
                    {
                        string targetUserName;
                        inEdge.SourceVertex.TryGetProperty<String>(out targetUserName, 1);
                        sb.AppendLine(String.Format(targetUserName));
                    }
                }
            }

            return sb.ToString();
        }
	}
}
