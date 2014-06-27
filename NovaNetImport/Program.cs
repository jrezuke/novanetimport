using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using NLog;

namespace NovaNetImport
{
    class Program
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        static void Main()
        {
            Logger.Info("Starting Novanet Import Service");

            //var basePath = AppDomain.CurrentDomain.BaseDirectory;

            //get sites and load into list of siteInfo 
            var sites = GetSites();

            //get the database column schema
            var dbColList = GetDbColumnSchema();

            //iterate sites
            foreach (var si in sites)
            {
                Console.WriteLine("Site: " + si.Name);
                //Get the folder file last dates
                GetFolderFilesLastDate(si);

                //get the folders and files not yet imported
                var folderAndFiles = GetFolderAndFiles(si);
                
                if (si.FolderFileLastDates.Count > 0)
                {
                    foreach (var folderFileList in folderAndFiles)
                    {
                        var folderName = folderFileList.Name;
                        

                        //if(si.FolderFileLastDates)
                        foreach (var file in folderFileList.Files)
                        {
                            Console.WriteLine("file name: " + file.Name);
                            var streamRdr = file.OpenText();
                            string line;
                            string[] colNameList = { };
                            var rows = 0;
                            while ((line = streamRdr.ReadLine()) != null)
                            {
                                var columns = line.Split(',');
                                if (rows == 0)
                                {
                                    colNameList = (string[])columns.Clone();
                                    rows++;
                                    continue;
                                }

                                //the sample_key_num column appears in the row 3 times - just capture the first appearance
                                var isFirst = true;
                                var bNoPatientId = false;
                                var subId = "";
                                for (int i = 0; i < columns.Length - 1; i++)
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
                                                    Logger.Warn("Warning: Could not extract subject id - file name:" +
                                                                file.FullName + ", row:" + rows);
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
                                                    Logger.Warn("Warning: Could not extract subject id - file name:" +
                                                                file.FullName + ", row:" + rows);
                                                }
                                                dbSubj.Value = subId;
                                            }
                                        }
                                        //if (colName == "result_str_val")
                                        //{
                                        //    if (string.IsNullOrEmpty(col))
                                        //    {
                                        //        col = "-999";
                                        //    }
                                        //}
                                    }
                                }

                                //special db columns
                                var dbColSpecial = dbColList.Find(x => x.Name == "computerName");

                                if (file.Directory != null)
                                {
                                    dbColSpecial.Value = file.Directory.Name;

                                    dbColSpecial = dbColList.Find(x => x.Name == "siteId");
                                    dbColSpecial.Value = si.Id.ToString(CultureInfo.InvariantCulture);

                                    dbColSpecial = dbColList.Find(x => x.Name == "fileName");
                                    dbColSpecial.Value = file.Name;

                                    InsertRowIntoDatabase(dbColList, file.Directory.Name, file.FullName, rows.ToString(CultureInfo.InvariantCulture));
                                }



                                rows++;

                            }
                        } //foreach (var file in folderFileList.Files)

                    } //foreach (var folderFileList in folderAndFiles)
                    UpdateLastFolderFileDates(si.FolderFileLastDates, si.Id);

                }//if(si.FolderFileLastdates.Count >0)
            }

            //Console.Read();
        }

        private static void UpdateLastFolderFileDates(IEnumerable<FolderFileLastDate> ffList, int siteId)
        {
            foreach (var folderFileLastDate in ffList)
            {
                DateTime? lastDateTime = null;
                //check for date change
                if (folderFileLastDate.LastFileDate == null)
                {
                    lastDateTime = folderFileLastDate.NewLastFileDate;
                }
                else
                {
                    if (folderFileLastDate.NewLastFileDate != null)
                    {
                        if (folderFileLastDate.LastFileDate.Value.CompareTo(folderFileLastDate.NewLastFileDate.Value) <= 0)
                        {
                            lastDateTime = folderFileLastDate.NewLastFileDate.Value;
                        }
                    }
                    else
                    {
                        continue;
                    }

                }

                if (folderFileLastDate.Id == 0)
                {
                    //insert new folder
                    AddNewFolderFileLastDate(siteId, folderFileLastDate.Name, lastDateTime);
                }
                else
                {
                    UpdateFolderFileLastDate(folderFileLastDate.Id, lastDateTime);
                }
            }
        }

        private static void UpdateFolderFileLastDate(int id, DateTime? lastDateTime)
        {
            var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            using (var conn = new SqlConnection(strConn))
            {
                var cmd = new SqlCommand
                {
                    Connection = conn,
                    CommandText = "UpdateNovanetFolderFileLastDate",
                    CommandType = CommandType.StoredProcedure
                };
                var param = new SqlParameter("@id", id);
                cmd.Parameters.Add(param);
                param = lastDateTime.HasValue ? new SqlParameter("@lastFileDate", lastDateTime) : new SqlParameter("@lastFileDate", DBNull.Value);
                cmd.Parameters.Add(param);

                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    var sMsg = "novanet AddNewFolderFileLastDate  - id: " + id + ", last DateTime: " + lastDateTime;
                    sMsg += ex.Message;
                    Logger.LogException(LogLevel.Error, sMsg, ex);
                }
                conn.Close();
            }
        }

        private static void AddNewFolderFileLastDate(int siteId, string folder, DateTime? lastDateTime)
        {
            var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            using (var conn = new SqlConnection(strConn))
            {
                var cmd = new SqlCommand
                          {
                              Connection = conn,
                              CommandText = "AddNovanetFolderFileLastDate",
                              CommandType = CommandType.StoredProcedure
                          };
                var param = new SqlParameter("@siteId", siteId);
                cmd.Parameters.Add(param);
                param = new SqlParameter("@folderName", folder);
                cmd.Parameters.Add(param);
                param = lastDateTime.HasValue ? new SqlParameter("@lastFileDate", lastDateTime) : new SqlParameter("@lastFileDate", DBNull.Value);
                cmd.Parameters.Add(param);

                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    var sMsg = "novanet AddNewFolderFileLastDate  - computer:" + folder + ", siteId: " + siteId + ", last DateTime: " + lastDateTime;
                    sMsg += ex.Message;
                    Logger.LogException(LogLevel.Error, sMsg, ex);
                }
                conn.Close();
            }



        }

        private static void GetFolderFilesLastDate(SiteInfo si)
        {
            var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            SqlDataReader rdr = null;
            using (var conn = new SqlConnection(strConn))
            {
                try
                {
                    var cmd = new SqlCommand
                    {
                        Connection = conn,
                        CommandText = "GetNovanetLastFileDates",
                        CommandType = CommandType.StoredProcedure
                    };
                    var param = new SqlParameter("@siteId", si.Id);
                    cmd.Parameters.Add(param);

                    conn.Open();
                    rdr = cmd.ExecuteReader();

                    si.FolderFileLastDates = new List<FolderFileLastDate>();
                    while (rdr.Read())
                    {
                        var ffld = new FolderFileLastDate();
                        int pos = rdr.GetOrdinal("Id");
                        ffld.Id = rdr.GetInt32(pos);

                        pos = rdr.GetOrdinal("MachineName");
                        ffld.Name = rdr.GetString(pos);

                        pos = rdr.GetOrdinal("LastFileDate");
                        ffld.LastFileDate = rdr.IsDBNull(pos) ? (DateTime?)null : rdr.GetDateTime(pos);

                        si.FolderFileLastDates.Add(ffld);
                    }
                    rdr.Close();

                }
                catch (Exception ex)
                {
                    Logger.Error(ex);
                }
                finally
                {
                    if (rdr != null)
                        rdr.Close();
                }
            }
        }

        private static List<DbNnColumn> GetDbColumnSchema()
        {
            //get the database column schema
            SqlDataReader rdr = null;
            var dbColList = new List<DbNnColumn>();
            var strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            using (var conn = new SqlConnection(strConn))
            {
                try
                {


                    var cmd = new SqlCommand("SELECT * FROM Novanet", conn);
                    conn.Open();

                    rdr = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
                    for (int i = 0; i < rdr.FieldCount; i++)
                    {
                        var col = new DbNnColumn
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
                }
                catch (Exception ex)
                {
                    Logger.Error(ex);
                }
                finally
                {
                    if (rdr != null)
                        rdr.Close();
                }
            }//using (var conn = new SqlConnection(strConn))
            return dbColList;
        }

        private static void InsertRowIntoDatabase(IEnumerable<DbNnColumn> dbColList, string machine, string file, string row)
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
                    SqlParameter param;
                    if (col.Name == "result_str_val")
                    {
                        param = string.IsNullOrEmpty(col.Value) ? new SqlParameter("@result_str_val", DBNull.Value) : new SqlParameter("@result_str_val", col.Value);
                    }
                    else
                        param = new SqlParameter("@" + col.Name, col.Value);

                    cmd.Parameters.Add(param);
                }
                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    var sMsg = "novanet import error - computer:" + machine + ", file: " + file + ", row: " + row;
                    sMsg += ex.Message;
                    Logger.LogException(LogLevel.Error, sMsg, ex);
                }
                conn.Close();
            }

        }

        private static IEnumerable<FolderFileList> GetFolderAndFiles(SiteInfo si)
        {
            var list = new List<FolderFileList>();

            //get the parent path for this site
            var parentPath = ConfigurationManager.AppSettings["NovaNetUploadPath"];
            parentPath = Path.Combine(parentPath, si.SiteId);

            if (Directory.Exists(parentPath))
            {
                //get the folders (named after the computer name) 
                var folders = Directory.EnumerateDirectories(parentPath);
                var newLastDate = new DateTime();

                foreach (var folder in folders)
                {
                    var folderAndFile = new FolderFileList();
                    list.Add(folderAndFile);

                    var di = new DirectoryInfo(folder);
                    folderAndFile.Name = di.Name;

                    DateTime? lastFileDate = null;

                    var lastFolderFileDate = si.FolderFileLastDates.Find(x => x.Name == di.Name);
                    if (lastFolderFileDate == null)
                    {
                        //if the site doesn't have this folder then add it
                        lastFolderFileDate = new FolderFileLastDate { Name = di.Name };
                        si.FolderFileLastDates.Add(lastFolderFileDate);
                    }
                    else
                    {
                        if (lastFolderFileDate.LastFileDate.HasValue)
                        {
                            lastFileDate = lastFolderFileDate.LastFileDate;
                        }
                    }

                    foreach (var file in di.GetFiles())
                    {
                        Console.WriteLine("file name: " + file.FullName);
                        if (!file.Name.ToUpper().StartsWith("PR"))
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
                        if (lastFileDate != null && lastFileDate.Value.CompareTo(fileDate) >= 0)
                            continue;

                        folderAndFile.Files.Add(file);

                        if (newLastDate.CompareTo(fileDate) < 0)
                            newLastDate = fileDate;

                    }//foreach (var file in di.GetFiles())

                    if (lastFolderFileDate.LastFileDate == null)
                        lastFolderFileDate.NewLastFileDate = newLastDate;
                    else
                    {
                        if (newLastDate.CompareTo(lastFolderFileDate.LastFileDate.Value) > 0)
                            lastFolderFileDate.NewLastFileDate = newLastDate;
                    }

                }//foreach (var folder in folders)


            }

            return list;
        }

        //private static IEnumerable<FileInfo> GetFileList(SiteInfo si, ref DateTime newLastDate)
        //{
        //    var list = new List<FileInfo>();

        //    //get the parent path for this site
        //    var parentPath = ConfigurationManager.AppSettings["NovaNetUploadPath"];
        //    parentPath = Path.Combine(parentPath, si.SiteId);

        //    if (Directory.Exists(parentPath))
        //    {
        //        //get the folders (named after the computer name) 
        //        var folders = Directory.EnumerateDirectories(parentPath);
        //        foreach (var folder in folders)
        //        {
        //            Console.WriteLine("Folder: " + folder);
        //            var di = new DirectoryInfo(folder);
        //            foreach (var file in di.GetFiles())
        //            {
        //                Console.WriteLine("file name: " + file.FullName);
        //                if (!file.Name.ToUpper().StartsWith("PR"))
        //                {
        //                    //skip all files except files that start with pr
        //                    //maybe archive file
        //                    continue;
        //                }

        //                //extract the date from the file name
        //                var datePart = file.Name.Substring(2, 6);
        //                var sDate = "20" + datePart.Substring(0, 2) + "/" + datePart.Substring(2, 2) + "/" +
        //                            datePart.Substring(4, 2);
        //                var fileDate = DateTime.Parse(sDate);

        //                Console.WriteLine(fileDate);
        //                if (si.LastFileDate.HasValue)
        //                {
        //                    //if the last date is greater than the file date
        //                    if (si.LastFileDate.Value.CompareTo(fileDate) >= 0)
        //                        continue;
        //                }
        //                if (newLastDate.CompareTo(fileDate) < 0)
        //                    newLastDate = fileDate;
        //                list.Add(file);
        //            }
        //        }
        //    }
        //    //get the file list

        //    return list;
        //}

        private static IEnumerable<SiteInfo> GetSites()
        {
            var sil = new List<SiteInfo>();

            String strConn = ConfigurationManager.ConnectionStrings["Halfpint"].ToString();
            SqlDataReader rdr = null;
            using (var conn = new SqlConnection(strConn))
            {
                try
                {
                    var cmd = new SqlCommand("", conn) { CommandType = CommandType.StoredProcedure, CommandText = "GetSitesActive" };

                    conn.Open();
                    rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var si = new SiteInfo();
                        var pos = rdr.GetOrdinal("ID");
                        si.Id = rdr.GetInt32(pos);

                        pos = rdr.GetOrdinal("Name");
                        si.Name = rdr.GetString(pos);

                        pos = rdr.GetOrdinal("SiteID");
                        si.SiteId = rdr.GetString(pos);

                        //pos = rdr.GetOrdinal("LastNovanetFileDateImported");
                        //si.LastFileDate = rdr.IsDBNull(pos) ? (DateTime?)null : rdr.GetDateTime(pos);

                        sil.Add(si);
                    }
                    rdr.Close();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex);
                }
                finally
                {
                    if (rdr != null)
                        rdr.Close();
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
        //public DateTime? LastFileDate { get; set; }
        public List<FolderFileLastDate> FolderFileLastDates { get; set; }
    }

    public class DbNnColumn
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public string FieldType { get; set; }
        public string Value { get; set; }
    }

    public class FolderFileLastDate
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime? LastFileDate { get; set; }
        public DateTime? NewLastFileDate { get; set; }
    }

    public class FolderFileList
    {
        public FolderFileList()
        {
            Files = new List<FileInfo>();
        }
        public string Name { get; set; }
        public List<FileInfo> Files { get; set; }
    }
}
