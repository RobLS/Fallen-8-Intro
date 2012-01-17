﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Fallen8.API;
using MySql.Data.MySqlClient;
using Fallen8.API.Index;
using Fallen8.API.Helper;
using Fallen8.Model;
using System.Diagnostics;

namespace Intro
{
    public class Import
    {        
        public static void ImportFromMySql(IFallen8 myFallen8, IIndex nodeIndex)
        {            
            #region Connect to MySql

            var connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3}", 
                Config.HOST, 
                Config.DATABASE, 
                Config.USER, 
                Config.PASSWORD);

            var connection = new MySqlConnection(connectionString);
            connection.Open();

            

            #endregion

            #region Import

            // import words
            Stopwatch sw = Stopwatch.StartNew();
            ReadWords(myFallen8, connection, nodeIndex, Config.TABLE_WORDS);
            Console.WriteLine("took {0} ms", sw.Elapsed.TotalMilliseconds);
            sw.Restart();
            ReadCooccurrences(myFallen8, connection, nodeIndex, Config.TABLE_CO_N, Config.CO_N_EDGE_PROPERTY_ID);
            Console.WriteLine("took {0} ms", sw.Elapsed.TotalMilliseconds);
            sw.Restart();
            ReadCooccurrences(myFallen8, connection, nodeIndex, Config.TABLE_CO_S, Config.CO_S_EDGE_PROPERTY_ID);
            Console.WriteLine("took {0} ms", sw.Elapsed.TotalMilliseconds);
            sw.Stop();

            #endregion

            #region Disconnect MySql

            connection.Close();

            #endregion
        }

        private static void ReadWords(IFallen8 myFallen8, MySqlConnection mySql, IIndex nodeIndex, String tableName)
        {            
            // query
            var query = mySql.CreateCommand();
            query.CommandText = "SELECT w_id, word FROM " + tableName;

            String word;
            Int32 w_id;
            DateTime creationDate = DateTime.Now;
            IVertexModel vertex;

            Console.WriteLine("importing {0} words from {1}", GetMySqlRowCount(mySql, tableName), tableName);

            var reader = query.ExecuteReader();

            while (reader.Read())
            {
                w_id = reader.GetInt32(0);
                word = reader.GetString(1);

                vertex = myFallen8.CreateVertex(
                       new VertexModelDefinition(creationDate)
                        .AddProperty(Config.W_ID_PROPERTY_ID, w_id)
                        .AddProperty(Config.WORD_PROPERTY_ID, word)
                );

                nodeIndex.AddOrUpdate(w_id, vertex);
            }

            reader.Close();
            Console.WriteLine("done");
        }

        private static void ReadCooccurrences(IFallen8 myFallen8, MySqlConnection mySql, IIndex nodeIdx, String tableName, long edgePropertyID)
        {
            // query
            var query = mySql.CreateCommand();
            query.CommandText = "SELECT w1_id, w2_id, freq, sig FROM " + tableName;

            DateTime creationDate = DateTime.Now;
            Int32 w1_id;
            Int32 w2_id;
            Int32 freq;
            Double sig;

            IEnumerable<IGraphElementModel> sources;
            IEnumerable<IGraphElementModel> targets;

            IVertexModel source = null;
            IVertexModel target = null;

            Console.WriteLine("importing {0} co-occurrences from {1}", GetMySqlRowCount(mySql, tableName), tableName);

            var reader = query.ExecuteReader();
        
            while (reader.Read())
            {
                w1_id = reader.GetInt32(0);
                w2_id = reader.GetInt32(1);
                freq = reader.GetInt32(2);
                sig = reader.GetDouble(3);

                if (nodeIdx.GetValue(out sources, w1_id))
                {
                    source = (IVertexModel) sources.First();
                }

                if (nodeIdx.GetValue(out targets, w2_id))
                {
                    target = (IVertexModel) targets.First();
                }

                // create edge

                myFallen8.CreateEdge(source.Id, edgePropertyID, new EdgeModelDefinition(target.Id, creationDate)
                    .AddProperty(Config.FREQ_PROPERTY_ID, freq)
                    .AddProperty(Config.SIG_PROPERTY_ID, sig));              
            }
            reader.Close();
            Console.WriteLine("done");
        }

        private static int GetMySqlRowCount(MySqlConnection mySql, String tableName = "")
        {            
            int count = 0;

            var query = mySql.CreateCommand();
            query.CommandText = "SELECT COUNT(*) FROM " + tableName;

            var reader = query.ExecuteReader();

            while (reader.Read())
            {
                count = reader.GetInt32(0);
            }

            reader.Close();

            return count;
        }
    }
}