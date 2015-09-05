using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Data;
using System.IO;
using CsvHelper;
namespace Neteeza_CSV
{
    class Program
    {
        private static string _ConnectionString = string.Empty;
        private static string _Csvout = string.Empty;
        private static int _CommandTimeout;
        private const string _NeteezaQuery =
                            @"select 	applicationname, masterappidshort,
                            sum(current_flag) Total,
                            sum(case when sub_type = 'QBO Sub' then current_flag else 0 end) QBOSub,
                            sum(case when sub_type = 'QBO Free' then current_flag else 0 end) QBOFree,
                            sum(case when sub_type = 'QBO Trial' then current_flag else 0 end) QBOTrial
                            from weekly_app_connections_detail 
                            where weekenddate = (select max(weekenddate) from weekly_app_connections_detail) and sub_type in ('QBO Sub', 'QBO Free', 'QBO Trial')
                            group by applicationname,masterappidshort
                            order by applicationname;";
        private static List<NeteezaFields> _listRecords = null;

        static void Main(string[] args)
        {
            _ConnectionString = System.Configuration.ConfigurationSettings.AppSettings["neteeza"];
            _Csvout = System.Configuration.ConfigurationSettings.AppSettings["csvout"];
            _CommandTimeout = ParseInt(System.Configuration.ConfigurationSettings.AppSettings["commandtimeout"].ToString());
            var result = ReadNeteeza(_ConnectionString, _NeteezaQuery);
            WriteCsv(result);
            Console.ReadLine();
        }

        private static int ParseInt(string p)
        {
            int result;
            int.TryParse(p, out result);
            return result;
        }

        private static void WriteCsv(List<NeteezaFields> netRecords)
        {
            try
            {
                _listRecords = netRecords.OrderBy(o => o.AppName).ToList();
            Console.WriteLine("\nWriting Files...");
            string pathString = _Csvout;
            string fileName = string.Format("{0}-{1:dd-MM-yyyy-HH-mm-ss}.csv", "App-Live-Connection", DateTime.Now);
            System.IO.Directory.CreateDirectory(pathString);
            pathString = System.IO.Path.Combine(pathString, fileName);
            using (var myStream = File.Open(pathString, System.IO.FileMode.Create))
            {
                using (var writer = new CsvWriter(new StreamWriter(myStream)))
                {
                    writer.Configuration.RegisterClassMap<NeteezaFieldsMap>();
                    writer.WriteRecords(_listRecords);
                }
            }
            Console.WriteLine("Path to my file: {0}\n", pathString);
            Console.WriteLine("Done");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static List<NeteezaFields> ReadNeteeza(string connString, string netzQuery)
        {
            try
            {
                _listRecords = new List<NeteezaFields>();
                using (OdbcConnection odbcConnection = new OdbcConnection(connString))
                {
               
                    odbcConnection.Open();
                    Console.WriteLine("Connection Estabilished with Neteeza...");
                    using (OdbcCommand odbcCommand = new OdbcCommand(netzQuery, odbcConnection))
                    {
                        odbcCommand.CommandTimeout = _CommandTimeout;
                        using (OdbcDataReader odbcDataReader = odbcCommand.ExecuteReader())
                        {
                            Console.WriteLine("Reading...");
                            while (odbcDataReader.Read())
                            {
                                var record = new NeteezaFields();
                                record.AppName = odbcDataReader.GetString(0);
                                record.MasterAppId = odbcDataReader.GetString(1);
                                record.Total = odbcDataReader.GetString(2);
                                record.QboSub = odbcDataReader.GetString(3);
                                record.QboFree = odbcDataReader.GetString(4);
                                record.QboTrial = odbcDataReader.GetString(5);
                                _listRecords.Add(record);
                            }
                        }
                    }
                    odbcConnection.Close();
                    Console.WriteLine("Connection Closed...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return _listRecords;
        }

        private static void TestMethod()
        {
            const string connString = "Driver=NetezzaSQL; Server={0}; Port=5480; Database={1}; Persist Security Info=true; UID={2}; PWD={3}";
            var connection = string.Format(connString, "qysprdntzdb05.data.bos.intuit.net", "UED_QBO_WS", "SMADHAVAN", "SMADHAVAN");
            using (OdbcConnection con = new OdbcConnection(connection))
            {
                con.Open();
                using (OdbcCommand oCmd = new OdbcCommand("select * from UED_QBO_WS..WEEKLY_APP_CONNECTIONS_DETAIL", con))
                {
                    using (OdbcDataReader oRead = oCmd.ExecuteReader())
                    {
                        while (oRead.Read())
                        {
                            var dataRead = oRead.GetString(0);
                        }
                    }
                }
            }
        }

        private static DataSet FillDataAdapter(DataSet currentDataset, string connectionString, string queryString)
        {
            using (OdbcConnection connection =
               new OdbcConnection(connectionString))
            {
                OdbcDataAdapter adapter =
                    new OdbcDataAdapter(queryString, connection);

                // Open the connection and fill the DataSet. 
                try
                {
                    connection.Open();
                    adapter.Fill(currentDataset);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                // The connection is automatically closed when the 
                // code exits the using block.
            }
            return currentDataset;
        }
    }
}
