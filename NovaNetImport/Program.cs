using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
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
                var newLastDate = DateTime.MinValue;
                IEnumerable<FileInfo> fileList = GetFileList(si, ref newLastDate);

                //get the column schema for checks insulin recommendation worksheet
                var dbColList = new List<DBnnColumn>();
                var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
                using (var conn = new SqlConnection(strConn))
                {
                    var cmd = new SqlCommand("SELECT * FROM Novanet", conn);
                    conn.Open();

                    var rdr = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                    for (int i = 0; i < rdr.FieldCount; i++)
                    {
                        var col = new DBnnColumn
                                  {
                            Name = rdr.GetName(i),
                            DataType = rdr.GetDataTypeName(i)
                        };

                        dbColList.Add(col);
                        var fieldType = rdr.GetFieldType(i);
                        if (fieldType != null)
                        {
                            col.FieldType = fieldType.ToString();
                        }

                        
                    }
                }//using (var conn = new SqlConnection(strConn))

                foreach (var file in fileList)
                {
                    if (file.Name.Contains("Copy"))
                        continue;   
                    var streamRdr = file.OpenText();
                    string line;
                    string[] colNameList= {};
                    var rows = 0;
                    while ((line = streamRdr.ReadLine()) != null)
                    {
                        var columns = line.Split(',');
                        if (rows == 0)
                        {
                            colNameList = (string[]) columns.Clone();
                            rows++;
                            continue;
                        }

                        //the sample_key_num column appears in the row 3 times - just capture the first appearance
                        var isFirst = true;
                        var bNoPatientId = false;
                        var subId = "";
                        for (int i=0; i<columns.Length-1; i++)
                        {
                            var col = columns[i];
                            var colName = colNameList[i];
                            if (colName == "sample_key_num")
                            {
                                if (isFirst)
                                    isFirst = false;
                                else
                                    continue;
                                
                            }
                            var dbCol = dbColList.Find(x => x.Name == colName);
                            if (dbCol != null)
                            {
                                Console.WriteLine("Col name: " + colName);
                                dbCol.Value = col;
                                if (colName == "patient_id")
                                {
                                    if (string.IsNullOrEmpty(col))
                                    {
                                        bNoPatientId = true;
                                    }
                                    else
                                    {
                                        var dbSubj = dbColList.Find(x => x.Name == "subjectId");
                                        if (col.Length == 9)
                                            subId = col.Substring(2, 2) + "-" + col.Substring(4, 4) + "-" +
                                                    col.Substring(8);
                                        else
                                        {
                                            Logger.Warn("Warning: Could not extract subject id - file name:" + file.FullName + ", row:" + rows);
                                        }
                                        dbSubj.Value = subId;
                                    }
                                }
                                if (colName == "medrec_num")
                                {
                                    if (bNoPatientId)
                                    {
                                        var dbSubj = dbColList.Find(x => x.Name == "subjectId");
                                        if (col.Length == 9 && col.StartsWith("HP"))
                                            subId = col.Substring(2, 2) + "-" + col.Substring(4, 4) + "-" +
                                                    col.Substring(8);
                                        else
                                        {
                                            Logger.Warn("Warning: Could not extract subject id - file name:" + file.FullName + ", row:" + rows);
                                        }
                                        dbSubj.Value = subId;
                                    }
                                }
                                if (colName == "result_str_val")
                                {
                                    if (string.IsNullOrEmpty(col))
                                    {
                                        col = "-999";
                                    }
                                }
                            }
                        }

                        //special db columns
                        var dbColSpecial = dbColList.Find(x => x.Name == "computerName");
                        if (file.Directory != null)
                        {
                            dbColSpecial.Value = file.Directory.Name;

                            dbColSpecial = dbColList.Find(x => x.Name == "siteId");
                            dbColSpecial.Value = si.Id.ToString();
                        
                            InsertRowIntoDatabase(dbColList, file.Directory.Name, file.FullName, rows.ToString());
                        }

                        rows++;

                    }
                }
            }

            Console.Read();
        }

        private static void InsertRowIntoDatabase(IEnumerable<DBnnColumn> dbColList, string machine, string file, string row)
        {
            var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            using (var conn = new SqlConnection(strConn))
            {
                var cmd = new SqlCommand
                          {
                              Connection = conn,
                              CommandText = "AddNovanetImport",
                              CommandType = CommandType.StoredProcedure
                          };
                foreach (var col in dbColList)
                {
                    if (col.Name == "Id")
                        continue;
                    var param = new SqlParameter("@" + col.Name, col.Value);
                    cmd.Parameters.Add(param);
                }
                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    var sMsg = "novanet import error - computer:" + machine + ", file: " + file + ", row: " + row ;
                    sMsg += ex.Message;
                    Logger.LogException(LogLevel.Error, sMsg, ex);
                }
                conn.Close();
            }
            
        }

        private static IEnumerable<FileInfo> GetFileList(SiteInfo si, ref DateTime newLastDate)
        {
            var list = new List<FileInfo>();

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
                            //skip all files except files that start with pr
                            //maybe archive file
                            continue;
                        }

                        //extract the date from the file name
                        var datePart = file.Name.Substring(2, 6);
                        var sDate = "20" + datePart.Substring(0, 2) + "/" + datePart.Substring(2, 2) + "/" +
                                    datePart.Substring(4, 2);
                        var fileDate = DateTime.Parse(sDate);
                        
                        Console.WriteLine(fileDate);
                        if (si.LastFileDate.HasValue)
                        {
                            //if the last date is greater than the file date
                            if (si.LastFileDate.Value.CompareTo(fileDate) >= 0)
                                continue;
                        }
                        if (newLastDate.CompareTo(fileDate) < 0)
                            newLastDate = fileDate;
                        list.Add(file);
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
                    var cmd = new SqlCommand("", conn) { CommandType = CommandType.StoredProcedure, CommandText = "GetSitesActive" };

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

    public class DBnnColumn
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public string FieldType { get; set; }
        public string Value { get; set; }
    }
}
