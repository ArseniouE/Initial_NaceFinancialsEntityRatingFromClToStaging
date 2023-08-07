using PGDAL;
using SQLDAL;
using System;
using System.Configuration;
using System.Data;
using System.IO;

namespace InitialMigr_EntityRatingFromCl
{
    public class Program
    {
        private static readonly log4net.ILog Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly string Path = ConfigurationManager.AppSettings["Path"].ToString();
        static void Main(string[] args)
        {
            Log.Info($"CL EntityRating  started at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
            try
            {
                //Init Data Contexts
                var postGreContext = new PostGreContext();
                var sqlContext = new SqlContext();
                
                //Execute Sequence

                DataTable entityratingdata = postGreContext.SelectEntityRatingFromCl();

                DataTable industrycodedata = postGreContext.SelectIndustryCode();

                DataTable financialsdata = postGreContext.SelectFinancials();

                if (entityratingdata.Rows.Count > 0)
                {
                    var result = sqlContext.SqlSequence(entityratingdata);

                    CheckSqlSequenceResult(result);
                }
                else
                {
                    Log.Debug("EntityRating Table from Cl was Empty.");              
                }

                if (industrycodedata.Rows.Count > 0)
                {
                    var result = sqlContext.SqlSequenceIndustryCode(industrycodedata);

                    CheckSqlSequenceResult(result);
                }
                else
                {
                    Log.Debug("Industry Table from Cl was Empty.");                 
                }

                if (financialsdata.Rows.Count > 0)
                {
                    var result = sqlContext.SqlSequenceFinancials(financialsdata);

                    CheckSqlSequenceResult(result);
                }
                else
                {
                    Log.Debug("Financials from Cl was Empty.");                  
                }

            }
            catch (Exception e)
            {
               // ExportResultsTxt("1");
                Log.Error("CL EntityRating failed:\n " + e.Message + "\n" + e.StackTrace);
            }
        }

        /// <summary>
        /// Checking the result if it is 0 or 1 and exports a .txt file.
        /// </summary>
        /// <param name="result"></param>
        private static void CheckSqlSequenceResult(string result)
        {
            if (result == "0")
            {
                Log.Info($"CL EntityRating executed successfully at: {DateTime.Now:dd-MM-yyyy H:mm:ss}");
               // ExportResultsTxt(result);
            }
            else
            {
                Log.Error("CL EntityRating failed:\n ");
              //  ExportResultsTxt(result);
            }
        }

        /// <summary>
        /// Exports a .txt file to the desired file path with result of the sequence 0 for success 1 for fail
        /// </summary>
        /// <param name="result"></param>
        private static void ExportResultsTxt(string result)
        {
            var path = $@"{Path}\CL_RatingScenario_Day_to_Day_Result_{DateTime.Now.ToShortDateString().Replace("/", "-")}_{DateTime.Now.Hour.ToString()}_{DateTime.Now.Minute.ToString()}.txt";

            using (StreamWriter sw = (File.Exists(path) ? File.AppendText(path) : File.CreateText(path)))
            {
                try
                {
                    sw.Write(result);
                    sw.Close();
                }
                catch (Exception e)
                {
                    Log.Error($"CL_RatingScenario_Day_to_Day_Result.txt export failed :\n" + e.Message + "\n" + e.StackTrace);
                }
            }
        }
    }
}
