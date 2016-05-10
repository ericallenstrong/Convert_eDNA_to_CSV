using System;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Convert_eDNA_To_CSV;
using InStep.eDNA.EzDNAApiNet;

namespace Convert_eDNA_To_CSV
{
    class Program
    {   
        //Main Program
        static void Main(string[] args)
        {
            Console.WriteLine(String.Format("Program initialized at {0}", DateTime.Now.ToString()));
            History.SetHistoryTimeout(2400);
            //Construct a list of tags to pull
            List<TagPull> tagPullList = ConstructTagsToPull(args);
            //Use threading to pull the tags
            var startPullTime = DateTime.Now;
            Console.WriteLine("Pulling list of tags using multithreading...");
            Parallel.ForEach(tagPullList, (tp, loopState) =>{tp.PullData();});
            //When finished, write the results
            Console.WriteLine("Pull completed successfully. Program will exit after next key press.");
            Console.ReadKey();
        }
        private static void StopProgram(string error)
        {
            //Do I really need a new method for this? Maybe not. But I wanted it to be standardized.
            Console.WriteLine(error);
            Console.WriteLine("Program terminated unsuccessfully. Program will exit after next key press.");
            Console.ReadKey();
            Environment.Exit(0);
        }
        //Construct the list of tags to pull
        private static List<TagPull> ConstructTagsToPull(string[] args)
        {
            var tagPullList = new List<TagPull>();
            string eDNAService = String.Empty;
            DateTime startDate = DateTime.MinValue;
            DateTime endDate = DateTime.MaxValue;
            int numMonthsBatch = 3;
            string outDir = String.Empty;
            Console.WriteLine("Validating user-supplied arguments...");
            //This function will make sure the user-supplied parameters are the proper format, something was actually passed, etc.
            ValidateArgs(args, out eDNAService, out startDate, out endDate, out numMonthsBatch, out outDir);
            //The idea behind this entire program is ease-of-use for large scale eDNA conversion. Don't supply the program with a list of eDNA points...
            //Instead, give it a service, and it will find and pull every single point in the service
            Console.WriteLine(String.Format("Retrieving the point list from eDNA service {0}...",eDNAService));
            string[] pointList = RetrievePointListFromDNA(eDNAService);           
            //This next bit of code just constructs a list of objects from the TagPull class- we will be iterating through it using a Parallel ForEach
            //later in the program
            Console.WriteLine("Constructing list of tags to pull...");
            foreach (string tagName in pointList)
            {
                //So why am I creating "batches" using the monthly batching interval? A few reasons. eDNA doesn't do well with long-term data pulls, often returning
                //an error if the pull goes on long enough. Also, using a batching interval means that more pulls can potentially be going on simultaneously in the
                //Parallel.ForEach. The downside is that the files will usually need to be concatenated after this program this complete. However, since I'm usually
                //just creating a database from these CSV files, it's really not that bad.
                for (DateTime curDate = startDate; curDate < endDate; curDate = curDate.AddMonths(numMonthsBatch))
                {
                    DateTime curEndDate = (curDate.AddMonths(numMonthsBatch) > endDate) ? endDate : curDate.AddMonths(numMonthsBatch);
                    tagPullList.Add(new TagPull(tagName, outDir, curDate, curEndDate));
                }               
            }
            return tagPullList;
        }
        //Validate the supplied parameters
        private static void ValidateArgs(string[] args, out string eDNAService, out DateTime startDate, out DateTime endDate, out int numMonthsBatch, out string outDir)
        {
            //This method may be a little bit haphazardly written, but it's really just validating user input.
            //These are all default parameters, which should hopefully always be overwritten (except for the outDir)
            eDNAService = String.Empty;
            startDate = new DateTime(2010, 1, 1);
            endDate = new DateTime(2020, 1, 1);
            numMonthsBatch = 3;
            outDir = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath) + "\\Data\\";
            //Parse the first supplied argument (the eDNA service), which is necessary
            if (args.Length == 0) { eDNAService = ValidateConsoleInput("eDNA Service:"); }
            else { eDNAService = args[0]; }
            //Parse the startDate (second supplied argument)
            string startDateString = String.Empty;
            if (args.Length < 2) { startDateString = ValidateConsoleInput("Start Date:"); }
            else { startDateString = args[1]; }
            //Try to convert the startDate to a correct DateTime
            try { startDate = Convert.ToDateTime(startDateString); }
            catch (FormatException) { StopProgram("ERROR- The start date (parameter 2) is not a properly formatted DateTime."); }
            //Parse the endDate (third supplied argument)
            string endDateString = String.Empty;
            if (args.Length < 3) endDateString = ValidateConsoleInput("End Date:");
            else { endDateString = args[2]; }
            //Try to convert the endDate to a correct DateTime
            try { endDate = Convert.ToDateTime(endDateString); }
            catch (FormatException) { StopProgram("ERROR- The end date (parameter 3) is not a properly formatted DateTime."); }
            //Check if the endDate is before the startDate
            if (startDate > endDate) StopProgram("ERROR- The end date is before the start date.");
            //Check the month batching interval
            string numMonthsBatchString = String.Empty;
            if (args.Length < 4) numMonthsBatchString = ValidateConsoleInput("Batch Interval (months):");
            //Now, try to convert the batching month interval
            try { numMonthsBatch = Convert.ToInt32(numMonthsBatchString); }
            catch (FormatException) { StopProgram("ERROR- The batching interval (parameter 4) is not a properly formatted integer."); }
            //Check if the output path was specified, otherwise write to the folder where the program is executing
            if (args.Length > 4) outDir = args[4];
            //Create the directory if it doesn't exist. A lot of exceptions could potentially happen here.
            try { Directory.CreateDirectory(outDir); }
            catch (IOException) { StopProgram("ERROR in directory name (parameter 4). The path is already defined as a file. Try another name."); }
            catch (UnauthorizedAccessException) { StopProgram("ERROR in directory name (parameter 4). Access to directory is not authorized."); }
            catch (ArgumentException) { StopProgram("ERROR in directory name (parameter 4).  Path is null or contains invalid characters."); }
            catch { StopProgram("ERROR- Unhandled exception in directory name (parameter 4)"); }
            Console.WriteLine(String.Format("Preparing to convert eDNA service {0} from {1} to {2}...", 
                eDNAService, startDate.ToString(), endDate.ToString()));
            Console.WriteLine(String.Format("Writing to directory {0}...", outDir));
        }
        private static string ValidateConsoleInput(string userPrompt)
        {
            //I just want to check that the user actually supplies something useful each time, not null or whitespace (is there a better way? Does this deserve its own function?)
            Console.Write(userPrompt);
            string promptAnswer = Console.ReadLine();
            if (String.IsNullOrWhiteSpace(promptAnswer)) StopProgram("No input supplied. Exiting program.");
            return promptAnswer;
        }
        //Retrieve the point list
        private static string[] RetrievePointListFromDNA(string service)
        {
            //I'm only interested in the pointIDs; is there a way to just return null?
            string[] pointIDs, pointTime, pointStatus, pointDesc, pointUnits;
            double[] pointValues;
            int nRet = Configuration.DnaGetPointListEx(ushort.MaxValue, service, 0, out pointIDs, out pointValues, out pointTime, out pointStatus, out pointDesc, out pointUnits);
            //In this next line, I'm filtering out the elements that are null or whitespace. Got to be a better way to do this, eDNA API sucks, applicable methods don't work, etc.
            string[] newPointIDs = pointIDs.ToList().Where(f => !String.IsNullOrWhiteSpace(f)).ToArray();
            Console.WriteLine(String.Format("Retrieved {0} points from {1}", newPointIDs.Length, service));
            return newPointIDs;
        }       
    }
    public class TagPull
    {
        public string DNATag { get; private set; }
        public string OutPath { get; private set; }
        public DateTime StartDate { get; private set; }
        public DateTime EndDate { get; private set; }
        public TagPull(string eDNATag, string outDir, DateTime startDate, DateTime endDate)
        {
            this.DNATag = eDNATag;
            //We're going to construct the entire output path from the start, since we already know all the necessary information supplied to the constructor
            //It is very important that the strings be formatted in a way that they can be used in the filename
            this.OutPath = Path.Combine(outDir, 
                String.Join("_", eDNATag, startDate.ToString("yyyy-dd-M--HH-mm-ss"), endDate.ToString("yyyy-dd-M--HH-mm-ss")) + ".csv");
            this.StartDate = startDate;
            this.EndDate = endDate;
        }
        public void PullData()
        {
            //These next few lines are just initialization for the required eDNA "out" parameters
            var startPullTime = DateTime.Now;
            Console.WriteLine(String.Format("Starting data pull for {0} between {1} and {2}...",
                this.DNATag, this.StartDate.ToString(), this.EndDate.ToString()));
            uint uiKey = 0;
            double dValue = 0;
            int dtTime;
            string strStatus = "";
            //The OutPath directory should always exist, because it is either the current executing directory, or it was created earlier in the code
            using (StreamWriter outfile = new StreamWriter(this.OutPath))
            {       
                //eDNA API performs threading inside, which is why I can wrap this in another threading layer (it takes time for the eDNA API to send
                //a network request, retrieve data back, etc.
                int result = History.DnaGetHistRawUTC(this.DNATag, Utility.GetUTCTime(this.StartDate), Utility.GetUTCTime(this.EndDate), out uiKey);
                //Any result other than 0 is "bad" or means that the data pull has ended. Eventually I should check for all options explicitly, maybe
                //allow a "re-start" of failed pulls?
                while (result == 0)
                {
                    //I want the function that retrieves the UTC time because that's exactly what I want to write. No need to waste time converting to UTC.
                    result = History.DnaGetNextHistUTC(uiKey, out dValue, out dtTime, out strStatus);
                    outfile.WriteLine(String.Join(",",dtTime.ToString(),dValue.ToString(),strStatus));
                }             
            }
            //This will call a function to gzip the file and remove the original file. These CSV files get really large, believe me, this is necessary
            this.GZipFile();
            //Success! It's important to cancel the histrequest, because it could cause slowdown on the server side checking to see if the requests are really closed
            History.DNACancelHistRequest(uiKey);
            Console.WriteLine("Finished data pull for {0} in {1} seconds.",this.DNATag,(DateTime.Now - startPullTime).TotalSeconds); 
        }
        private void GZipFile()
        {
            //These next few lines were basically just copied from MDSN- look here for help:
            //https://msdn.microsoft.com/en-us/library/ms404280%28v=vs.110%29.aspx
            var fileToCompress = new FileInfo(this.OutPath);
            using (FileStream originalFileStream = fileToCompress.OpenRead())
            {
                if ((File.GetAttributes(fileToCompress.FullName) &
                   FileAttributes.Hidden) != FileAttributes.Hidden & fileToCompress.Extension != ".gz")
                {
                    using (FileStream compressedFileStream = File.Create(fileToCompress.FullName + ".gz"))
                    {
                        using (GZipStream compressionStream = new GZipStream(compressedFileStream, CompressionMode.Compress))
                        {
                            originalFileStream.CopyTo(compressionStream);
                        }
                    }
                }
            }
            //We don't need the original file any longer, it just takes up space
            File.Delete(this.OutPath);
        }
    }
}
