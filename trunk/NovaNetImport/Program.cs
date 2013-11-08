using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Security.Policy;
using NLog;

namespace NovaNetImport
{
    class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            Logger.Info("Starting Novanet Import Service");
            
            var basePath = AppDomain.CurrentDomain.BaseDirectory;

            //get sites and load into list of siteInfo 
            var sites = GetSites();

            //iterate sites
            foreach (var si in sites)
            {
                Console.WriteLine("Site: " + si.Name);
                //get file list not yet imported
                List<string> fileList = GetFileList(si);
                
            }

            Console.Read();
        }

        private static List<string> GetFileList(SiteInfo si)
        {
            var list = new List<string>();
            
            //get the parent path for this site
            var parentPath = ConfigurationManager.AppSettings["NovaNetUploadPath"];
            parentPath = Path.Combine(parentPath, si.SiteId);

            if (Directory.Exists(parentPath))
            {
                //get the folders (named after the computer name) 
                var folders = Directory.EnumerateDirectories(parentPath);

                foreach (var folder in folders)
                {
                    Console.WriteLine("Folder: " + folder);
                    var di = new DirectoryInfo(folder);
                    foreach (var file in di.GetFiles())
                    {
                        Console.WriteLine("file name: " + file.FullName);
                        if (! file.Name.ToUpper().StartsWith("PR"))
                        {
                            //archive file
                            continue;
                        }

                        //extract the date from the file name
                        var datePart = file.Name.Substring(2, 6);
                        var sDate = "20" + datePart.Substring(0, 2) + "/" + datePart.Substring(2, 2) + "/" +
                                    datePart.Substring(4, 2);
                        var fileDate = DateTime.Parse(sDate);
                        Console.WriteLine(fileDate);
                    }
                }
            }
            //get the file list

            return list;
        }

        private static IEnumerable<SiteInfo> GetSites()
        {
            var sil = new List<SiteInfo>();

            String strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();

            using (var conn = new SqlConnection(strConn))
            {
                try
                {
                    var cmd = new SqlCommand("", conn) { CommandType = System.Data.CommandType.StoredProcedure, CommandText = "GetSitesActive" };

                    conn.Open();
                    var rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var si = new SiteInfo();
                        var pos = rdr.GetOrdinal("ID");
                        si.Id = rdr.GetInt32(pos);

                        pos = rdr.GetOrdinal("Name");
                        si.Name = rdr.GetString(pos);

                        pos = rdr.GetOrdinal("SiteID");
                        si.SiteId = rdr.GetString(pos);

                        pos = rdr.GetOrdinal("LastNovanetFileDateImported");
                        si.LastFileDate = rdr.IsDBNull(pos) ? (DateTime?) null : rdr.GetDateTime(pos);

                        sil.Add(si);
                    }
                    rdr.Close();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex);
                }
            }
            return sil;
        }
    }

    public class SiteInfo
    {
        public int Id { get; set; }
        public string SiteId { get; set; }
        public string Name { get; set; }
        public DateTime? LastFileDate { get; set; }
    }
}
