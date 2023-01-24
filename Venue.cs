using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace OpenAlexDataExtractor
{
    public class Venue
    {
        [BsonElement("venue_id")]
        public string id { get; set; }
        [BsonElement("name")]
        public string display_name { get; set; }

        public UInt64 GetIdNumber()
        {
            return GetIdNumber(id);
        }

        UInt64 GetIdNumber(string idStr)
        {
            if (id == null)
                return 0;

            string[] idParts = idStr.Split('/');
            UInt64 idNumber = Convert.ToUInt64(idParts[idParts.Length - 1].Substring(1));
            return idNumber;
        }

        Byte[] GetNameBytes()
        {
            return Encoding.UTF8.GetBytes(display_name);
        }

        public void Serialize(Int64 venuePos, BinaryWriter dataWriter, BinaryWriter indexWriter)
        {
            indexWriter.Write(GetIdNumber());
            indexWriter.Write(venuePos);

            Byte[] nameBytes = GetNameBytes();
            UInt16 nameLength = Convert.ToUInt16(nameBytes.Length);
            dataWriter.Write(GetIdNumber());
            dataWriter.Write(nameLength);
            dataWriter.Write(nameBytes);
        }

        public static void Extract(string inputPath, string outputPath)
        {
            string venuesPath = Path.Combine(inputPath, "venues");
            DirectoryInfo diVenuesPath = new DirectoryInfo(venuesPath);
            foreach (var venueDir in diVenuesPath.GetDirectories())
            {
                foreach (FileInfo venueFile in venueDir.GetFiles())
                {
                    if (venueFile.Extension == ".gz")
                    {
                        Console.WriteLine(venueFile.FullName);
                        using (FileStream venueDataFile = new FileStream(Path.Combine(outputPath, $"venue-data.wjf"), FileMode.Append))
                        {
                            using (BinaryWriter venueDataWriter = new BinaryWriter(venueDataFile))
                            {
                                using (FileStream venueIndexFile = new FileStream(Path.Combine(outputPath, $"venue-index.wjf"), FileMode.Append))
                                {
                                    using (BinaryWriter venueIndexWriter = new BinaryWriter(venueIndexFile))
                                    {
                                        using (FileStream gzFile = File.OpenRead(venueFile.FullName))
                                        {
                                            using (GZipStream gzStream = new GZipStream(gzFile, CompressionMode.Decompress))
                                            {
                                                using (StreamReader reader = new StreamReader(gzStream))
                                                {
                                                    string? line = reader.ReadLine();
                                                    while (line != null)
                                                    {
                                                        try
                                                        {
                                                            Venue venue = JsonConvert.DeserializeObject<Venue>(line);
                                                            venue.Serialize(venueDataFile.Position, venueDataWriter, venueIndexWriter);
                                                        }
                                                        catch
                                                        {

                                                        }
                                                        line = reader.ReadLine();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        
                    }
                }
            }
        }
    }
}
