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
            if (args.Length > 0)
            {
                //args[0]
                //"Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi"
                string connStr = "Provider=OraOLEDB.Oracle;Data Source=NEPSTRN;User Id=NEPSBI;Password=xs2nepsbi";
                OleDbConnection conn = new OleDbConnection(connStr);
                conn.Open();

                Console.WriteLine("Starting Procedure");

                #region Procedure

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

                #endregion

                Console.WriteLine("Procedure Completed");

                OleDbCommand cmd_Segment = new OleDbCommand();
                cmd_Segment.Connection = conn;
                cmd_Segment.CommandText = "SELECT DISTINCT GDS_SEGMENT FROM REF_BI_GDS_SEGMENT";
                cmd_Segment.CommandType = CommandType.Text;
                OleDbDataReader dr_Segment = cmd_Segment.ExecuteReader();

                while (dr_Segment.Read())
                {                    
                    string segment = dr_Segment.GetString(0);

                    OleDbCommand cmd_SegmentDetails = new OleDbCommand();
                    cmd_SegmentDetails.Connection = conn;
                    cmd_SegmentDetails.CommandText = "SELECT ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_TYPE, PARENT_IPID, AREA_TYPE, ROWID FROM BI_SERV_BOUND WHERE SEGMENT = :sgm AND BI_BATCH_ID IS NULL";
                    cmd_SegmentDetails.Parameters.AddWithValue(":sgm", segment);
                    cmd_SegmentDetails.CommandType = CommandType.Text;
                    OleDbDataReader dr_SegmentDetails = cmd_SegmentDetails.ExecuteReader();

                    if (dr_SegmentDetails.HasRows)
                    {
                        Console.WriteLine("Processing segment {0}", segment);
                        List<string> lines = new List<string>();

                        #region StartTime

                        OleDbCommand cmd_GetBID = new OleDbCommand();
                        cmd_GetBID.Connection = conn;
                        cmd_GetBID.CommandText = "SELECT BI_BATCH_SEQ.NEXTVAL AS BID FROM DUAL";
                        cmd_GetBID.CommandType = CommandType.Text;
                        OleDbDataReader dr_BID = cmd_GetBID.ExecuteReader();

                        //get batch id
                        dr_BID.Read();
                        string bid = dr_BID.GetDecimal(0).ToString();
                        dr_BID.Close();
                        cmd_GetBID.Dispose();

                        OleDbCommand cmd_SetStartTime = new OleDbCommand();
                        cmd_SetStartTime.Connection = conn;
                        cmd_SetStartTime.CommandText = "INSERT INTO BI_BATCH(BATCH_ID, INSTANCE_ID, CLASS_NAME, TIME_START, SERVICE_NAME, TYPE, FILE_HAS_ERROR) VALUES(:bid, 'GDS_ServiceBoundary', 'EdgeFrontier.GDS.OBServiceBoundary', SysDate, 'GDS', 'OUTBOUND', 0)";
                        cmd_SetStartTime.Parameters.AddWithValue(":bid", bid);
                        cmd_SetStartTime.CommandType = CommandType.Text;
                        cmd_SetStartTime.ExecuteNonQuery();
                        cmd_SetStartTime.Dispose();

                        #endregion

                        while (dr_SegmentDetails.Read())
                        {
                            string ACTION_TYPE = dr_SegmentDetails.GetString(0);
                            string FEAT_TYPE = dr_SegmentDetails.GetString(1);
                            string EXC_ABB = dr_SegmentDetails.GetString(2);
                            string IPID = dr_SegmentDetails.GetDecimal(3).ToString();
                            string BND_TYPE = dr_SegmentDetails.GetString(4);
                            string PARENT_IPID = (!dr_SegmentDetails.IsDBNull(5)) ? dr_SegmentDetails.GetDecimal(5).ToString() : "";
                            string AREA_TYPE = (!dr_SegmentDetails.IsDBNull(6)) ? dr_SegmentDetails.GetDecimal(6).ToString() : "";
                            string ROWID = dr_SegmentDetails.GetString(7);

                            Console.WriteLine("{0} is the checking value", IPID);

                            string line = String.Format("{0}|{1}|{2}|{3}|{4}|{5}|", ACTION_TYPE, FEAT_TYPE, EXC_ABB, IPID, BND_TYPE, PARENT_IPID);

                            OleDbCommand cmd_Coor = new OleDbCommand();
                            cmd_Coor.Connection = conn;
                            cmd_Coor.CommandText = "SELECT COOR_X, COOR_Y, ROWID FROM BI_SERV_COOR WHERE IPID = :ipid_val";
                            cmd_Coor.Parameters.AddWithValue(":ipid_val", IPID);
                            cmd_Coor.CommandType = CommandType.Text;
                            OleDbDataReader dr_Coor = cmd_Coor.ExecuteReader();

                            Console.WriteLine("pass ipid section, now onto reading");

                            if (dr_Coor.HasRows)
                            {
                                ArrayList coor_list = new ArrayList();

                                while (dr_Coor.Read())
                                {
                                    string COOR_X = dr_Coor.GetDecimal(0).ToString();
                                    string COOR_Y = dr_Coor.GetDecimal(1).ToString();
                                    string ROWID_CHILD = dr_Coor.GetString(2);

                                    coor_list.Add(new string[] { COOR_X, COOR_Y });

                                    #region UpdateChild

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

                                Console.WriteLine("Done Reading Coordinate");

                                int size = coor_list.Count;

                                for (int i = 0; i < size; ++i)
                                {
                                    String[] coors = (String[])coor_list[i];
                                    line += (i == 0 ? "" : "\n") + coors[0] + "|" + coors[1] + (i < size - 1 ? "|" : "");
                                }
                            }

                            dr_Coor.Close();
                            cmd_Coor.Dispose();

                            Console.WriteLine("Adding lines");
                            lines.Add(line + "|" + AREA_TYPE);

                            Console.WriteLine("Updating parent");

                            #region UpdateParent

                            OleDbCommand cmd_GetBIO = new OleDbCommand();
                            cmd_GetBIO.Connection = conn;
                            cmd_GetBIO.CommandText = "SELECT BI_INSERT_SEQ.NEXTVAL FROM BI_SERV_BOUND WHERE ROWNUM = 1";
                            cmd_GetBIO.CommandType = CommandType.Text;
                            OleDbDataReader dr_BIO = cmd_GetBIO.ExecuteReader();

                            dr_BIO.Read();
                            string bio = dr_BIO.GetDecimal(0).ToString();
                            dr_BIO.Close();
                            cmd_GetBIO.Dispose();

                            OleDbCommand cmd_UpdateDeletion = new OleDbCommand();
                            cmd_UpdateDeletion.Connection = conn;
                            cmd_UpdateDeletion.CommandText = "UPDATE BI_SERV_BOUND set BI_BATCH_ID = :bid_val, BI_INSERT_ORDER = :bio_val where rowid = :rowid_val";
                            cmd_UpdateDeletion.Parameters.AddWithValue(":bid_val", bid);
                            cmd_UpdateDeletion.Parameters.AddWithValue(":bio_val", bio);
                            cmd_UpdateDeletion.Parameters.AddWithValue(":rowid_val", ROWID);
                            cmd_UpdateDeletion.ExecuteNonQuery();
                            cmd_UpdateDeletion.Dispose();

                            #endregion

                            Console.WriteLine("Updating parent completed");
                        }

                        string date = DateTime.Now.ToString("yyyyMMdd");
                        string filename = segment + "_DailyBND_" + date + ".csv";

                        if (File.Exists(filename))
                        {
                            File.Delete(filename);
                        }

                        File.AppendAllLines(filename, lines);

                        #region EndTime

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

                    dr_SegmentDetails.Close();
                    cmd_SegmentDetails.Dispose();
                }

                dr_Segment.Close();
                cmd_Segment.Dispose();
                conn.Dispose();
                conn.Close();
            }
            else
            {
                Console.WriteLine("Please enter connection string.\nExample: \"Provider = OraOLEDB.Oracle; Data Source = NEPSTRN; User Id = NEPSBI; Password = xs2nepsbi\"");
            }
        }
    }
}