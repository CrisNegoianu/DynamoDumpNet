using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Microsoft.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace DynamoDumpNet
{
    class Program
    {
        private static string _tableName;
        private static string _fileName;
        private static string _awsProfile;
        private static string _awsRegion;
        private static bool _overwrite;
        private static bool _useDynamoDBLocal;
        private static string _localServiceURL = "http://localhost:8000";

        private static AmazonDynamoDBClient _client;
        private static DescribeTableResponse _describeTableResponse;
        private static Table _table;

        static void Main(string[] args)
        {
            //args = "-m restore -t DynamoDBTableName -p local -r us-east-1 -l true".Split(' ');
            args = "-m restore -t ServiceHubDev -f DynamoDBData.json -p local -r us-east-1 -l http://192.168.99.100:8000".Split(' ');


            CommandLineApplication app = new CommandLineApplication();
            app.Name = "DynamoDump.Net";
            app.Description = ".NET Core application that can backup and restore dynamodb tables to/from json files";

            app.HelpOption("-?|-h|--help");

            CommandOption modeOption = app.Option("-m|--mode <backup,restore>", "Configures the DynamoDB table operation", CommandOptionType.SingleValue);
            CommandOption tableOption = app.Option("-t|--table <table-name>", "Configures the DynamoDB table to restore/backup", CommandOptionType.SingleValue);
            CommandOption fileOption = app.Option("-f|--file <import-file>", "Configures the json file used to restore/backup data", CommandOptionType.SingleValue);
            CommandOption profileOption = app.Option("-p|--profile <profile-name>", "The AWS profile from the credentials file to use when connecting to AWS. If not supplied the default profile is used", CommandOptionType.SingleValue);
            CommandOption regionOption = app.Option("-r|--region <region-code>", "The AWS region to use when connecting to AWS. If not supplied the default region configured in the credentials file is used", CommandOptionType.SingleValue);
            CommandOption localOption = app.Option("-l|--local <serviceURL>", "If set it attempts to restore/backup to/from a local instance of DynamoDB. If not supplied false is assumsed", CommandOptionType.SingleValue);
            CommandOption overwriteOption = app.Option("-o|--overwrite ", "If set the destination file will be overwritten if it exists when backing up data", CommandOptionType.SingleValue);

            app.OnExecute(async () =>
            {
                if (!tableOption.HasValue())
                {
                    ShowInfo("Table argument is mandatory");
                    return 0;
                }

                if (!fileOption.HasValue())
                {
                    ShowWarning("File option not supplied. Using DynamoDBData.json for the file name");
                    _fileName = "DynamoDBData.json";
                }
                else
                {
                    _fileName = fileOption.Value();
                }

                if (localOption.HasValue())
                {
                    ShowWarning("Using local DynamoDB");
                    _useDynamoDBLocal = true;

                    if (!("true".Equals(localOption.Value(), StringComparison.InvariantCultureIgnoreCase)))
                    {
                        _localServiceURL = localOption.Value();
                    }
                }

                if (!profileOption.HasValue())
                {
                    ShowWarning("Profile option not supplied. Using the default profile");
                    _awsProfile = "default";
                }
                else
                {
                    _awsProfile = profileOption.Value();
                }

                if (!regionOption.HasValue())
                {
                    ShowWarning("Region option not supplied. Using region eu-west-2");
                    _awsRegion = "eu-west-2";
                }
                else
                {
                    _awsRegion = regionOption.Value();
                }

                if (overwriteOption.HasValue())
                {
                    _overwrite = true;
                }

                _tableName = tableOption.Value();

                if (modeOption.HasValue())
                {
                    string mode = modeOption.Value();

                    if (mode.Equals("restore", StringComparison.InvariantCultureIgnoreCase))
                    {
                        await Restore();
                    }
                    else if (mode.Equals("backup", StringComparison.InvariantCultureIgnoreCase))
                    {
                        //ShowInfo("Backup mode not implemented yet.");
                        await Backup();
                    }
                    else
                    {
                        ShowError("Unknown mode {0}. ", mode);
                    }
                }
                else
                {
                    app.ShowHint();
                }

                CleanUp();

                return 0;
            });

            //app.Command("restore-command", (command) =>
            //{
            //    command.Description = "Restore the content of a DynamoDB table from a json file";
            //    command.HelpOption("-?|-h|--help");

            //    command.OnExecute(() =>
            //    {
            //        Console.WriteLine("simple-command has finished.");
            //        return 0;
            //    });
            //});

            app.Execute(args);

#if DEBUG
            Console.ReadKey();
#endif
        }

        private static async Task Restore()
        {
            ShowInfo("Restoring data for table {0} from file {1}", _tableName, _fileName);

            // First, read in the JSON data from the moviedate.json file
            StreamReader sr = null;
            JsonTextReader jtr = null;
            JArray recordsArray = null;

            try
            {
                sr = new StreamReader(_fileName);
                jtr = new JsonTextReader(sr);
                recordsArray = (JArray)JToken.ReadFrom(jtr);
            }
            catch (Exception ex)
            {
                ShowError("Error: could not read data from file '{0}'. {1}", _fileName, ex.Message);
                PauseForDebugWindow();
                return;
            }
            finally
            {
                if (jtr != null)
                {
                    jtr.Close();
                }
                if (sr != null)
                {
                    sr.Close();
                }
            }

            // Get a Table object for the table that you created in Step 1
            if (!await GetTableObject(_tableName, true))
            {
                PauseForDebugWindow();
                return;
            }

            // Load the movie data into the table (this could take some time)
            ShowInfo("Sending {0:#,##0} records to DynamoDB", recordsArray.Count);

            for (int i = 0, j = 99; i < recordsArray.Count; i++)
            {
                try
                {
                    string itemJson = recordsArray[i].ToString();
                    Document doc = Document.FromJson(itemJson);
                    await _table.PutItemAsync(doc);
                }
                catch (Exception ex)
                {
                    ShowError("Error writing record number #{0:#,##0}. {1}", i, ex.Message);
                    PauseForDebugWindow();
                    return;
                }

                if (i >= j)
                {
                    j++;
                    ShowInfo("{0,5:#,##0}, ", j.ToString());
                    if (j % 1000 == 0)
                    {
                        ShowInfo("\n                 ");
                    }
                    j += 99;
                }
            }

            ShowInfo("Finished writing all records to DynamoDB!");
            PauseForDebugWindow();
        }

        private static async Task Backup()
        {
            ShowInfo("Backing up data for table {0} to file {1}", _tableName, _fileName);

            if (File.Exists(_fileName))
            {
                if (_overwrite)
                {
                    ShowWarning("Deleting existing backup file");

                    try
                    {
                        File.Delete(_fileName);
                    }
                    catch (Exception ex)
                    {
                        ShowError("There was an error deliting existing backup file {0}. {1}", _fileName, ex.Message);
                        return;
                    }
                }
                else
                {
                    ShowError("File {0} alredy exists. To avoid overwriting data, please delete it manually first, or supplied the -o argument to overwrite it", _fileName);
                    return;
                }
            }

            // Get a Table object for the table that you created in Step 1
            if (!await GetTableObject(_tableName, false))
            {
                PauseForDebugWindow();
                return;
            }

            // Load the movie data into the table (this could take some time)
            ShowInfo("Scanning DynamoDB table", _describeTableResponse.Table.ItemCount);

            ScanRequest scanRequest = new ScanRequest(_tableName)
            {
                Select = Select.ALL_ATTRIBUTES
            };
            ScanResponse scanResponse = await _client.ScanAsync(scanRequest);

            int totalItems = scanResponse.Items.Count;

            ShowInfo("Writting {0} item to backup file", totalItems);

            using (var writer = File.CreateText(_fileName))
            {
                writer.WriteLine("[");

                int itemIndex = 0;

                foreach (var item in scanResponse.Items)
                {
                    itemIndex++;

                    Document document = Document.FromAttributeMap(item);
                    string json = document.ToJsonPretty();

                    await writer.WriteLineAsync(json + (itemIndex < totalItems ? "," : string.Empty));
                }

                writer.Write("]");
            }

            ShowInfo("Finished importing {0} records from DynamoDB", scanResponse.Items.Count);
            PauseForDebugWindow();
        }

        private static void CleanUp()
        {
            _client = null;
            _describeTableResponse = null;
            _table = null;
        }

        private static void PauseForDebugWindow()
        {
            //#if !DEBUG
            //            // Keep the console open if in Debug mode...
            //            Console.Write("\nPress any key to continue...");
            //            Console.ReadKey();
            //#endif
        }

        private static string GetFullErrorMessage(Exception ex)
        {
            string errorMessage = ex.Message;
            Exception innerException = ex.InnerException;
            while (innerException != null)
            {
                errorMessage = innerException.Message + Environment.NewLine + errorMessage;
                innerException = innerException.InnerException;
            }

            return errorMessage;
        }

        public static async Task<bool> GetTableObject(string tableName, bool isRestore)
        {
            if (!CreateClient())
            {
                return false;
            }

            var request = new DescribeTableRequest
            {
                TableName = tableName
            };

            try
            {
                _describeTableResponse = await _client.DescribeTableAsync(request);
            }
            catch (Exception ex)
            {
                ShowError("Failed to get table {0}'s details. {1}", tableName, GetFullErrorMessage(ex));
                return false;
            }

            if (_describeTableResponse?.Table == null)
            {
                _describeTableResponse = null;
                ShowError("Table {0} doesn't exist", tableName);
                return false;
            }


            //Only restore data into empty table, to avoid restoring into a live environment
            if (isRestore && _describeTableResponse.Table.ItemCount > 0)
            {
                ShowError("Cannot restore data into table {0} because it contains {1} records already. You can only restore data in empty tables", _tableName, _describeTableResponse.Table.ItemCount);
                return false;
            }

            // Now, create a Table object for the specified table
            try
            {
                _table = Table.LoadTable(_client, tableName);
            }
            catch (Exception ex)
            {
                ShowError("Error: failed to load the '{0}' table. {1}", tableName, ex.Message);
                return false;
            }

            return true;
        }

        public static bool CreateClient()
        {
            if (_useDynamoDBLocal)
            {
                //parse ip address and port
                int i = _localServiceURL.Contains("://") ? _localServiceURL.IndexOf("://") : -3;
                string ipAddress = _localServiceURL.Substring(i + 3);

                i = ipAddress.Contains(":") ? ipAddress.IndexOf(":") : -1;
                if (i == -1)
                {
                    ShowError("ERROR: Port not found. Please specify a port");
                    return false;
                }
                string portText = ipAddress.Substring(i + 1);
                ipAddress = ipAddress.Substring(0, i);
                int port = 0;
                if (!int.TryParse(portText, out port))
                {
                    ShowError("ERROR: Port is not a number. Please specify a valid port");
                    return false;
                }

                // First, check to see whether anyone is listening on the DynamoDB local port
                // (by default, this is port 8000, so if you are using a different port, modify this accordingly)
                bool localFound = false;
                try
                {
                    using (var tcp_client = new TcpClient())
                    {
                        var result = tcp_client.BeginConnect(ipAddress, port, null, null);
                        localFound = result.AsyncWaitHandle.WaitOne(3000); // Wait 3 seconds
                        tcp_client.EndConnect(result);
                    }
                }
                catch
                {
                    localFound = false;
                }
                if (!localFound)
                {
                    ShowError("ERROR: DynamoDB Local does not appear to have been started. Checked port " + port);
                    return (false);
                }

                CredentialProfileStoreChain chain = new CredentialProfileStoreChain();
                AWSCredentials credentials;

                if (!chain.TryGetAWSCredentials(_awsProfile, out credentials))
                {
                    ShowError("Profile {0} not found", _awsProfile);
                    return false;
                }

                RegionEndpoint region = RegionEndpoint.GetBySystemName(_awsRegion);

                if (region == null)
                {
                    ShowError("Region {0} not found", _awsRegion);
                    return false;
                }

                // If DynamoDB-Local does seem to be running, so create a client
                ShowInfo("Setting up a DynamoDB-Local client (DynamoDB Local seems to be running)");
                AmazonDynamoDBConfig ddbConfig = new AmazonDynamoDBConfig();
                ddbConfig.ServiceURL = _localServiceURL;

                try
                {
                    //_client = new AmazonDynamoDBClient(ddbConfig);
                    _client = new AmazonDynamoDBClient(credentials, ddbConfig);
                }
                catch (Exception ex)
                {
                    ShowError("FAILED to create a DynamoDBLocal client; " + ex.Message);
                    return false;
                }
            }

            else
            {
                CredentialProfileStoreChain chain = new CredentialProfileStoreChain();
                AWSCredentials credentials;

                if (!chain.TryGetAWSCredentials(_awsProfile, out credentials))
                {
                    ShowError("Profile {0} not found", _awsProfile);
                    return false;
                }

                RegionEndpoint region = RegionEndpoint.GetBySystemName(_awsRegion);

                if (region == null)
                {
                    ShowError("Region {0} not found", _awsRegion);
                    return false;
                }

                //If MFA is enabled on this probile, ask for an access code
                AssumeRoleAWSCredentials assumeCredentials = credentials as AssumeRoleAWSCredentials;
                if (assumeCredentials != null && !string.IsNullOrWhiteSpace(assumeCredentials.Options.MfaSerialNumber))
                {
                    assumeCredentials.Options.MfaTokenCodeCallback = () =>
                    {
                        Console.WriteLine("Enter MFA code: ");
                        return Console.ReadLine();
                    };
                }

                try
                {
                    ShowInfo("Connecting to DynamoDB with profile {0} in region {1}", _awsProfile, _awsRegion);
                    _client = new AmazonDynamoDBClient(credentials, region);
                }
                catch (Exception ex)
                {
                    ShowError("     FAILED to create a DynamoDB client; " + ex.Message);
                    return false;
                }
            }
            return true;
        }

        private static void ShowInfo(string message, params object[] parameters)
        {
            string nowText = DateTime.Now.ToString("hh:MM:ss");

            //ConsoleColor foregroundColor = Console.ForegroundColor;
            //ConsoleColor backgroundColor = Console.BackgroundColor;

            //Console.ForegroundColor = ConsoleColor.Red;
            //Console.BackgroundColor = ConsoleColor.White;

            Console.WriteLine(nowText + " - " + message, parameters);

            //Console.ForegroundColor = foregroundColor;
            //Console.BackgroundColor = backgroundColor;
        }
        private static void ShowWarning(string errorMessage, params object[] parameters)
        {
            string nowText = DateTime.Now.ToString("hh:MM:ss");

            ConsoleColor foregroundColor = Console.ForegroundColor;
            //ConsoleColor backgroundColor = Console.BackgroundColor;

            Console.ForegroundColor = ConsoleColor.Yellow;
            //Console.BackgroundColor = ConsoleColor.White;

            Console.WriteLine(nowText + " - " + errorMessage, parameters);

            Console.ForegroundColor = foregroundColor;
            //Console.BackgroundColor = backgroundColor;
        }

        private static void ShowError(string errorMessage, params object[] parameters)
        {
            string nowText = DateTime.Now.ToString("hh:MM:ss");

            ConsoleColor foregroundColor = Console.ForegroundColor;
            //ConsoleColor backgroundColor = Console.BackgroundColor;

            Console.ForegroundColor = ConsoleColor.Red;
            //Console.BackgroundColor = ConsoleColor.White;

            Console.WriteLine(nowText + " - " + errorMessage, parameters);

            Console.ForegroundColor = foregroundColor;
            //Console.BackgroundColor = backgroundColor;
        }
    }
}
