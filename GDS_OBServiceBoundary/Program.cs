using System;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Collections;
using System.Collections.Generic;

namespace GDS_OBServiceBoundary
{
    class Program
    {
        static void Main(string[] args)
        {
            string path = Directory.GetCurrentDirectory();
            string file_error = "error.txt";
            string error = "";

            if (File.Exists(Path.Combine(path, file_error))) File.Delete(Path.Combine(path, file_error));

            if (args.Length > 0)
            {
                #region 1. Get & Open Connection

                //"Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi"
                string connStr = args[0];
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                #endregion

                #region 2. Call Procedure

                try
                {
                    OleDbCommand cmd_callProcDisable = new OleDbCommand("NEPS.DISABLE_ALL_TRIGGER", conn);
                    cmd_callProcDisable.CommandType = CommandType.StoredProcedure;
                    cmd_callProcDisable.ExecuteNonQuery();
                    cmd_callProcDisable.Dispose();

                    OleDbCommand cmd_callProcedure = new OleDbCommand("GDS_OUTBOUND_SERVICE_BOUNDARY", conn);
                    cmd_callProcedure.CommandType = CommandType.StoredProcedure;
                    cmd_callProcedure.ExecuteNonQuery();
                    cmd_callProcedure.Dispose();

                    OleDbCommand cmd_callProcEnable = new OleDbCommand("NEPS.ENABLE_ALL_TRIGGER", conn);
                    cmd_callProcEnable.CommandType = CommandType.StoredProcedure;
                    cmd_callProcEnable.ExecuteNonQuery();
                    cmd_callProcEnable.Dispose();
                }
                catch (Exception ex)
                {
                    error += ex.ToString();
                }

                #endregion

                #region 3. Process

                #region 3.1. Find list of segments as reference

                OleDbCommand cmd_Segment = new OleDbCommand();
                cmd_Segment.Connection = conn;
                cmd_Segment.CommandText = "SELECT DISTINCT GDS_SEGMENT FROM REF_BI_GDS_SEGMENT";
                cmd_Segment.CommandType = CommandType.Text;
                OleDbDataReader dr_Segment = cmd_Segment.ExecuteReader();

                #endregion

                #region 3.2. Starts referencing data from segments

                while (dr_Segment.Read())
                {                    
                    string segment = dr_Segment.GetString(0);
                    
                    #region 3.2.2. Referencing data from main table

                    OleDbCommand cmd_SegmentDetails = new OleDbCommand();
                    cmd_SegmentDetails.Connection = conn;
                    cmd_SegmentDetails.CommandText = "SELECT ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_TYPE, PARENT_IPID, AREA_TYPE, ROWID FROM BI_SERV_BOUND WHERE SEGMENT = :sgm AND BI_BATCH_ID IS NULL AND TO_CHAR(PROCESSED_DATE, 'YYYYMMDD') >= '20201210'";
                    cmd_SegmentDetails.Parameters.AddWithValue(":sgm", segment);
                    cmd_SegmentDetails.CommandType = CommandType.Text;
                    OleDbDataReader dr_SegmentDetails = cmd_SegmentDetails.ExecuteReader();

                    #endregion

                    #region 3.2.3 Starts processing if main segment has related value in main table
                    
                    if (dr_SegmentDetails.HasRows)
                    {
                        List<string> lines = new List<string>();
                        
                        #region 3.2.2.1 Get Batch ID

                        OleDbCommand cmd_GetBID = new OleDbCommand();
                        cmd_GetBID.Connection = conn;
                        cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                        cmd_GetBID.CommandType = CommandType.Text;
                        OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();
                        
                        dr_BID.Read();
                        string bid = dr_BID.GetDecimal(0).ToString();
                        dr_BID.Close();
                        cmd_GetBID.Dispose();

                        #endregion

                        #region 3.2.2.2 Record StartTime

                        OleDbCommand cmd_SetStartTime = new OleDbCommand();
                        cmd_SetStartTime.Connection = conn;
                        cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_ServiceBoundary', 'EdgeFrontier.GDS.OBServiceBoundary', SysDate, 'GDS', 'OUTBOUND', 0)";
                        cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetStartTime.CommandType = CommandType.Text;
                        cmd_SetStartTime.ExecuteNonQuery();
                        cmd_SetStartTime.Dispose();

                        #endregion

                        #region 3.2.2.3 Starts reading and processing in details

                        while (dr_SegmentDetails.Read())
                        {
                            //Store data from executed query into variables
                            string ACTION_TYPE = dr_SegmentDetails.GetString(0);
                            string FEAT_TYPE = dr_SegmentDetails.GetString(1);
                            string EXC_ABB = dr_SegmentDetails.GetString(2);
                            string IPID = dr_SegmentDetails.GetDecimal(3).ToString();
                            string BND_TYPE = dr_SegmentDetails.GetString(4);
                            string PARENT_IPID = (!dr_SegmentDetails.IsDBNull(5)) ? dr_SegmentDetails.GetDecimal(5).ToString() : "";
                            string AREA_TYPE = (!dr_SegmentDetails.IsDBNull(6)) ? dr_SegmentDetails.GetDecimal(6).ToString() : "";
                            string ROWID = dr_SegmentDetails.GetString(7);

                            //Prepare first part of line
                            string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|coor=[", ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_TYPE, PARENT_IPID);

                            #region 3.2.2.3.1 Referencing data from Child table

                            OleDbCommand cmd_Coor = new OleDbCommand();
                            cmd_Coor.Connection = conn;
                            cmd_Coor.CommandText = "SELECT COOR_X, COOR_Y, ROWID FROM BI_SERV_COOR WHERE IPID = :ipid_val ORDER BY BI_INSERT_ORDER";
                            cmd_Coor.Parameters.AddWithValue(":ipid_val", IPID);
                            cmd_Coor.CommandType = CommandType.Text;
                            OleDbDataReader dr_Coor = cmd_Coor.ExecuteReader();

                            #endregion

                            #region 3.2.2.3.2 Starts processing if parent table has related values in child table

                            if (dr_Coor.HasRows)
                            {
                                ArrayList coor_list = new ArrayList();

                                #region 3.2.2.3.2.1 Starts referencing data from parent

                                while (dr_Coor.Read())
                                {
                                    string COOR_X = dr_Coor.GetDecimal(0).ToString();
                                    string COOR_Y = dr_Coor.GetDecimal(1).ToString();
                                    string ROWID_CHILD = dr_Coor.GetString(2);

                                    //Add data from the child table to be appended to the current line
                                    coor_list.Add(new string[] { COOR_X, COOR_Y });

                                    #region Update child table

                                    OleDbCommand cmd_GetBIOChild = new OleDbCommand();
                                    cmd_GetBIOChild.Connection = conn;
                                    cmd_GetBIOChild.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_SERV_COOR WHERE ROWNUM = 1";
                                    cmd_GetBIOChild.CommandType = CommandType.Text;
                                    OleDbDataReader dr_BIOChild = cmd_GetBIOChild.ExecuteReader();

                                    dr_BIOChild.Read();
                                    string bioChild = dr_BIOChild.GetDecimal(0).ToString();
                                    dr_BIOChild.Close();
                                    cmd_GetBIOChild.Dispose();

                                    OleDbCommand cmd_UpdateCoor = new OleDbCommand();
                                    cmd_UpdateCoor.Connection = conn;
                                    cmd_UpdateCoor.CommandText = "UPDATE BI_SERV_COOR SET BI_INSERT_ORDER = :bio WHERE ROWID = :rid";
                                    cmd_UpdateCoor.Parameters.AddWithValue(":bio", bioChild);
                                    cmd_UpdateCoor.Parameters.AddWithValue(":rid", ROWID_CHILD);
                                    cmd_UpdateCoor.ExecuteNonQuery();
                                    cmd_UpdateCoor.Dispose();

                                    #endregion
                                }

                                #endregion

                                #region 3.2.2.3.2.2 Append list of data from child table to the current line

                                int size = coor_list.Count;

                                for (int i = 0; i < size; ++i)
                                {
                                    String[] coors = (String[])coor_list[i];
                                    line += (i == 0 ? "" : "\n") + coors[0] + "|" + coors[1] + (i < size - 1 ? "|" : "");
                                }

                                #endregion
                            }

                            #endregion

                            #region 3.2.2.3.3 Close child table reader and dispose cursor

                            dr_Coor.Close();
                            cmd_Coor.Dispose();

                            #endregion

                            //Add data to list of output lines
                            lines.Add(line + "]|" + AREA_TYPE);

                            #region 3.2.2.3.4 Update main table

                            OleDbCommand cmd_GetBIO = new OleDbCommand();
                            cmd_GetBIO.Connection = conn;
                            cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_SERV_BOUND WHERE ROWNUM = 1";
                            cmd_GetBIO.CommandType = CommandType.Text;
                            OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                            dr_BIO.Read();
                            string bio = dr_BIO.GetDecimal(0).ToString();
                            dr_BIO.Close();
                            cmd_GetBIO.Dispose();

                            OleDbCommand cmd_UpdateMain = new OleDbCommand();
                            cmd_UpdateMain.Connection = conn;
                            cmd_UpdateMain.CommandText = "UPDATE BI_SERV_BOUND set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                            cmd_UpdateMain.Parameters.AddWithValue(":bid_val", bid);
                            cmd_UpdateMain.Parameters.AddWithValue(":bio_val", bio);
                            cmd_UpdateMain.Parameters.AddWithValue(":rowid_val", ROWID);
                            cmd_UpdateMain.ExecuteNonQuery();
                            cmd_UpdateMain.Dispose();

                            #endregion
                        }

                        #endregion

                        #region 3.2.2.4 Write CSV file

                        string date = DateTime.Now.ToString("yyyyMMdd");
                        string filename = segment + "_DailyBND_" + date + ".csv";

                        if (File.Exists(filename))
                        {
                            File.Delete(filename);
                        }

                        //Write data to file
                        File.AppendAllLines(filename, lines);

                        #endregion

                        #region 3.2.2.5 Record EndTime

                        OleDbCommand cmd_SetEndTime = new OleDbCommand();
                        cmd_SetEndTime.Connection = conn;
                        cmd_SetEndTime.CommandText = "UPDATE BI_BATCH SET TIME_END = SysDate, FILENAME = :filename WHERE BATCH_ID = :bid";
                        cmd_SetEndTime.Parameters.AddWithValue(":filename", filename);
                        cmd_SetEndTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetEndTime.CommandType = CommandType.Text;
                        cmd_SetEndTime.ExecuteNonQuery();
                        cmd_SetEndTime.Dispose();

                        #endregion
                    }
                    
                    #endregion

                    #region 3.2.4. Close main table reader and dispose cursor

                    dr_SegmentDetails.Close();
                    cmd_SegmentDetails.Dispose();

                    #endregion
                }

                #endregion

                #endregion

                #region 4. Close Connection

                dr_Segment.Close();
                cmd_Segment.Dispose();
                conn.Dispose();
                conn.Close();

                #endregion
            }
            else
            {
                error += "Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"";
            }

            if(error != "")
            {
                using (StreamWriter sw = File.AppendText(Path.Combine(path, file_error)))
                {
                    sw.WriteLine(DateTime.Now + Environment.NewLine + error + Environment.NewLine);
                }
            }
        }
    }
}