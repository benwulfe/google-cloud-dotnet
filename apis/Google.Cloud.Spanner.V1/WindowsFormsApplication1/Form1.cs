using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Transactions;
using System.Windows.Forms;
using Google.Cloud.Spanner;
using Google.Cloud.Spanner.V1;

namespace WindowsFormsApplication1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }
        static readonly Random s_rnd = new Random(Environment.TickCount);
        private static bool onceTrue = false;

        private async void button1_Click(object sender, EventArgs e)
        {
            TestExtensions.SessionGetter = (session) =>
            {
                if (onceTrue || (s_rnd != null && s_rnd.Next(100) < 50))
                {
                    onceTrue = true;
                    //use a valid expired session.
                    return new SessionName(session.SessionName.ProjectId, session.SessionName.InstanceId,
                        session.SessionName.DatabaseId, "AGxixuX72Phx7LDjnAe8a-AJKEs9lg_XO-avnpjYAWgqXcn-V4xuQ1DagA8");
                }
                return session.SessionName;
            };

            SpannerConnection.ConnectionPoolOptions.LogPerformanceTraces = true;
            SpannerConnection.ConnectionPoolOptions.PerformanceTraceLogInterval = 10000;
            SpannerConnection.ConnectionPoolOptions.ResetPerformanceTracesEachInterval = false;
            button1.Text = @"Working!";
            button1.Enabled = false;
//            using (var scope = new TransactionScope())
//            {
                using (var connection = new SpannerConnection("Data Source=spanneref/myspanner/mydatabase"))
                {
                    await connection.OpenAsync();
                    // QUERY EXAMPLE
                    var cmd = connection.CreateSelectCommand("SELECT * FROM Books ORDER BY ID");
//                    using (var tx = await connection.BeginTransactionAsync())
//                    {
//                        cmd.Transaction = tx;
                        long idCheck = 1;
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                if (reader.GetInt64(0) != idCheck)
                                {
                                    Debug.WriteLine(
                                        $"Id Check failed for Name: {reader["Name"]}  idCheck: {idCheck} readerValue: {reader.GetInt64(0)}");
                                    idCheck = reader.GetInt64(0);
                                }
                                idCheck++;
                            }
                        }

                        long maxId = 0;
                        int rowsAffected;

                        for (int i = 0; i < 2; i++)
                        {
                            //QUERY SCALAR
                            cmd = connection.CreateSelectCommand("SELECT MAX(ID) FROM Books");
//                            cmd.Transaction = tx;
                            maxId = await cmd.ExecuteScalarAsync<long>();
                            maxId += i + 1;

                            // INSERT EXAMPLE
                            cmd = connection.CreateInsertCommand("Books", new SpannerParameterCollection {
                                {"ID", SpannerDbType.Int64},
                                {"Author", SpannerDbType.String},
                                {"ISBN", SpannerDbType.String},
                                {"Name", SpannerDbType.String},
                                {"PublishDate", SpannerDbType.Date},
                            });
//                            cmd.Transaction = tx;

                            button1.Text = $@"writing {maxId}";

                            cmd.Parameters["ID"].Value = maxId;
                            cmd.Parameters["Author"].Value = "Phil Fritzche";
                            cmd.Parameters["ISBN"].Value = "12345678";
                            cmd.Parameters["Name"].Value = $"Testing Spanner using Examples #{maxId}";
                            cmd.Parameters["PublishDate"].Value = DateTime.Today;
                            rowsAffected = await cmd.ExecuteNonQueryAsync();
                            Debug.WriteLine($"{rowsAffected} Rows affected.");
                        }
//                        tx.Commit();
//                    }

                    // UPDATE EXAMPLE
                    cmd = connection.CreateUpdateCommand("Books", new SpannerParameterCollection {
                        {"ID", SpannerDbType.Int64},
                        {"Author", SpannerDbType.String},
                        {"ISBN", SpannerDbType.String},
                        {"Name", SpannerDbType.String},
                        {"PublishDate", SpannerDbType.Date},
                    });

                    cmd.Parameters["ID"].Value = maxId;
                    cmd.Parameters["Author"].Value = "Phil Fritzche";
                    cmd.Parameters["ISBN"].Value = "12345678";
                    cmd.Parameters["Name"].Value = $"Testing Spanner using Examples. {maxId} Edition";
                    cmd.Parameters["PublishDate"].Value = DateTime.Today;
                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    Debug.WriteLine($"{rowsAffected} Rows affected. MaxId={maxId}");
                }
//                scope.Complete();
//            }

            button1.Text = @"GO!";
            button1.Enabled = true;
        }
    }
}
