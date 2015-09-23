/**
*  --> Objective of the program is to connect to neteeza database using odbc connection
*  --> Supply the connection string parameter in app.config
*  --> Supply the csv out  parameter in app.config
*  --> Modify the connection time out if necessary.
*  --> Run the console program, Spit out csv file with date-time stamp 
*  --> Upload file to cloud
*  
* @param neteeza
* @param csvout
* @param commandtimeout
* @author Sumod Madhavan 
* @Date   9/7/2015 MM/DD/YYYY
 * 
 * Note : Make sure your odbc connection architecutre match with your program. Example ODBC X64 = Program Running on target X64
*/
#region <<Namespace>>
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Odbc;
using System.Data;
using System.IO;
using CsvHelper;
using System.Net;
using System.Collections.Specialized;
#endregion
namespace Neteeza_CSV
{
    class Program
    {
        #region <<class level variables>>
        //connection string
        private static string _ConnectionString = string.Empty;
        //path of the csv file
        private static string _Csvout = string.Empty;
        private static string _UploadUrl = string.Empty;
        //time out for odbc connection
        private static int _CommandTimeout;
        //sql query
        /// <summary>
        /// Referrence Query 1 :
        //# @"select 	applicationname, masterappidshort,
        //#                            sum(current_flag) Total,
        //#                            sum(case when sub_type = 'QBO Sub' then current_flag else 0 end) QBOSub,
        //#                            sum(case when sub_type = 'QBO Free' then current_flag else 0 end) QBOFree,
        //#                            sum(case when sub_type = 'QBO Trial' then current_flag else 0 end) QBOTrial
        //#                            from weekly_app_connections_detail 
        //#                            where weekenddate = (select max(weekenddate) from weekly_app_connections_detail) and sub_type in ('QBO Sub', 'QBO Free', 'QBO Trial')
        //#                            group by applicationname,masterappidshort
        //#                            order by applicationname;";
        /// </summary>
        private const string _NeteezaQuery =
                            @"select a.applicationname, a.masterappidshort, b.apptoken,
                            sum(current_flag) Total,
                            sum(case when sub_type = 'QBO Sub' then current_flag else 0 end) QBOSub,
                            sum(case when sub_type = 'QBO Free' then current_flag else 0 end) QBOFree,
                            sum(case when sub_type = 'QBO Trial' then current_flag else 0 end) QBOTrial
                            from weekly_app_connections_detail a , load_partnerapp_appmaster b
                            where sub_type in ('QBO Sub', 'QBO Free', 'QBO Trial') and weekenddate = (select max(weekenddate) from weekly_app_connections_detail) and 
                            a.MASTERAPPID = b.MASTERAPPID 
                            group by a.applicationname,a.masterappidshort,b.apptoken 
                            order by a.applicationname";
        //datastructure to hold live connections.
        private static List<NeteezaFields> _LiveConnectionRecords = null;
        #endregion
        /// <summary>
        /// Programm main starts.
        /// 
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            _ConnectionString = System.Configuration.ConfigurationSettings.AppSettings["neteeza"];
            _Csvout = System.Configuration.ConfigurationSettings.AppSettings["csvout"];
            _CommandTimeout = ParseInt(System.Configuration.ConfigurationSettings.AppSettings["commandtimeout"].ToString());
            _UploadUrl = System.Configuration.ConfigurationSettings.AppSettings["uploadurl"];
            var liveConnections = ReadNeteeza(_ConnectionString, _NeteezaQuery);
            if (liveConnections.Count > 0)
            {
                var filePath = WriteCsv(liveConnections);
                UploadFile(filePath);
            }
            else
            {
                Message("No records to write.");
            }
            Console.ReadLine();
        }
        /// <summary>
        /// the routines will upload the csv to cloud
        /// </summary>
        /// <param name="uploadPath">path of the file</param>
        private static void UploadFile(string uploadPath)
        {
            Message("Commencing file upload...");
            const string param = "name";
            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.UseDefaultCredentials = true;
                    NameValueCollection parameters = new NameValueCollection();
                    parameters.Add(param, Path.GetFileName(uploadPath));
                    webClient.QueryString = parameters;
                    byte[] result = webClient.UploadFile(_UploadUrl, uploadPath);
                    string responseAsString = Encoding.Default.GetString(result);
                    Message(string.Format("Response from Server : {0}",responseAsString));
                    Message("Done!");
                }
            }
            catch (Exception ex)
            {
                Message(ex.Message);
            }
        }
        #region <<Routines>>
        /// <summary>
        /// Parse string to integer.
        /// </summary>
        /// <param name="p"></param>
        /// <returns></returns>
        private static int ParseInt(string p)
        {
            int result;
            int.TryParse(p, out result);
            return result;
        }
        /// <summary>
        /// Writing the data retrieved from netezza to csv file.
        /// </summary>
        /// <param name="netRecords"></param>
        private static string WriteCsv(List<NeteezaFields> netRecords)
        {
            var pathString = string.Empty;
            try
            {
                _LiveConnectionRecords = netRecords.OrderBy(o => o.Total).ToList();
                Message("\nWriting Files...");
                pathString = _Csvout;
                string fileName = string.Format("{0}-{1:dd-MM-yyyy-HH-mm-ss}.csv", "Appliveconnections", DateTime.Now);
                System.IO.Directory.CreateDirectory(pathString);
                pathString = System.IO.Path.Combine(pathString, fileName);
                using (var myStream = File.Open(pathString, System.IO.FileMode.Create))
                {
                    using (var writer = new CsvWriter(new StreamWriter(myStream)))
                    {
                        writer.Configuration.RegisterClassMap<NeteezaFieldsMap>();
                        writer.WriteRecords(_LiveConnectionRecords);
                    }
                }
                Message(string.Format("Path to my file: {0}\n", pathString));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return pathString;
        }
        /// <summary>
        /// Pretty Print
        /// </summary>
        /// <param name="message"></param>
        private static void Message(string message)
        {
            Console.WriteLine(message);
        }
        /// <summary>
        /// Reading the data from neteeza.Require jump box connection
        /// to retrieve the data.
        /// Time and Space complexity = O(N) f(n) == g(n)
        /// </summary>
        /// <param name="connString">ODBC connection string</param>
        /// <param name="netzQuery">Posgresql query</param>
        /// <returns></returns>
        private static List<NeteezaFields> ReadNeteeza(string connString, string netzQuery)
        {
            try
            {
                _LiveConnectionRecords = new List<NeteezaFields>();
                using (OdbcConnection odbcConnection = new OdbcConnection(connString))
                {
                    try
                    {
                        odbcConnection.Open();
                        Message("We are doing good.Connection estabilished with neteeza.");
                    }
                    catch (Exception ex)
                    {
                        Message("OOPS!,Connection is rejected");
                        Message(ex.Message);
                    }
                    using (OdbcCommand odbcCommand = new OdbcCommand(netzQuery, odbcConnection))
                    {
                        odbcCommand.CommandTimeout = _CommandTimeout;
                        Message("Processing...");
                        using (OdbcDataReader odbcDataReader = odbcCommand.ExecuteReader())
                        {
                            //Cache the ordinal for better performance.
                            var rowOrdinals = new
                            {
                                AppName = odbcDataReader.GetOrdinal("APPLICATIONNAME"),
                                MasterAppId = odbcDataReader.GetOrdinal("MASTERAPPIDSHORT"),
                                AppToken = odbcDataReader.GetOrdinal("APPTOKEN"),
                                Total = odbcDataReader.GetOrdinal("TOTAL"),
                                QboSub = odbcDataReader.GetOrdinal("QBOSUB"),
                                QboFree = odbcDataReader.GetOrdinal("QBOFREE"),
                                QboTrial = odbcDataReader.GetOrdinal("QBOTRIAL")
                            };
                            Message("Reading...");
                            while (odbcDataReader.Read())
                            {
                                #region <<Debug>>
                                /*
                                var record = new NeteezaFields();
                                record.AppName = odbcDataReader.GetString(0);
                                record.MasterAppId = odbcDataReader.GetString(1);
                                record.Total = odbcDataReader.GetString(2);
                                record.QboSub = odbcDataReader.GetString(3);
                                record.QboFree = odbcDataReader.GetString(4);
                                record.QboTrial = odbcDataReader.GetString(5);
                                _LiveConnectionRecords.Add(record);*/
                                #endregion
                                _LiveConnectionRecords.Add
                                    (
                                    new NeteezaFields
                                    {
                                        AppName = odbcDataReader.GetString(rowOrdinals.AppName),
                                        MasterAppId = odbcDataReader.GetString(rowOrdinals.MasterAppId),
                                        AppToken = odbcDataReader.GetString(rowOrdinals.AppToken),
                                        Total = odbcDataReader.GetString(rowOrdinals.Total),
                                        QboSub = odbcDataReader.GetString(rowOrdinals.QboSub),
                                        QboFree = odbcDataReader.GetString(rowOrdinals.QboFree),
                                        QboTrial = odbcDataReader.GetString(rowOrdinals.QboTrial)
                                    }
                                    );
                            }
                        }
                    }
                    odbcConnection.Close();
                    Message("Connection closed.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Message(string.Format("Total number of records : {0}", _LiveConnectionRecords.Count));
            return _LiveConnectionRecords;
        }
        #endregion
    }
}
