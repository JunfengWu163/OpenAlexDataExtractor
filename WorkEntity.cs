using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenAlexDataExtractor
{
    public class WorkEntity
    {
        public UInt64 id { get; private set; }
        public UInt16 year { get; private set; }
        public UInt64 venueId { get; private set; }
        public string title { get; private set; }
        public List<(UInt64, double)> concepts { get; private set; } = new List<(UInt64, double)>();
        public List<UInt64> references { get; private set; } = new List<UInt64>();
        public List<(UInt64, UInt16)> authorships { get; private set; } = new List<(UInt64, UInt16)>();

        public WorkEntity(UInt64 idNumber, UInt64 numBuckets, string path)
        {
            UInt64 idxBucket = idNumber % numBuckets;
            string indexFileName = Path.Combine(path, $"work-sorted_index-{idxBucket}.wjf");
            Int64 workPos = FindWork(indexFileName, idNumber);
            if (workPos >= 0)
            {
                string dataFileName = Path.Combine(path, $"work-data-{idxBucket}.wjf");
                Deserialize(dataFileName, idNumber, workPos);
            }
            else
            {
                id = 0;
            }
        }

        Int64 FindWork(string indexFileName, UInt64 idNumber)
        {
            if (File.Exists(indexFileName))
            {
                using (FileStream indexFile = File.OpenRead(indexFileName))
                {
                    Int64 numIndices = indexFile.Length >> 4;
                    if (numIndices == 0)
                    {
                        return -1;
                    }

                    Int64 iStart = 0, iEnd = numIndices - 1;
                    using (BinaryReader reader = new BinaryReader(indexFile))
                    {
                        UInt64 indexStart = reader.ReadUInt64();
                        if (idNumber == indexStart)
                        {
                            return reader.ReadInt64();
                        }
                        else if (idNumber < indexStart)
                        {
                            return -1;
                        }

                        indexFile.Seek(iEnd << 4, SeekOrigin.Begin);
                        UInt64 indexEnd = reader.ReadUInt64();
                        if (idNumber == indexEnd)
                        {
                            return reader.ReadInt64();
                        }
                        else if (idNumber > indexEnd)
                        {
                            return -1;
                        }

                        while (iEnd - iStart >= 2)
                        {
                            Int64 iMiddle = (iStart + iEnd) >> 1;
                            indexFile.Seek(iMiddle << 4, SeekOrigin.Begin);
                            UInt64 indexMiddle = reader.ReadUInt64();
                            if (idNumber == indexMiddle)
                            {
                                return reader.ReadInt64();
                            }
                            else if (idNumber < indexMiddle)
                            {
                                iEnd = iMiddle;
                                indexEnd = indexMiddle;
                            }
                            else
                            {
                                iStart = iMiddle;
                                indexStart = indexMiddle;
                            }
                        }

                        // now iEnd - iStart < 2, but idNumber != indexStart && idNumber != indexEnd, thus idNumber is not found
                        return -1;
                    }
                }
            }
            return -1;
        }

        void Deserialize(string fileName, UInt64 idNumber, Int64 workPos)
        {
            using (FileStream dataFile = File.OpenRead(fileName))
            {
                dataFile.Seek(workPos, SeekOrigin.Begin);
                using (BinaryReader dataReader = new BinaryReader(dataFile))
                {
                    //dataWriter.Write(idNumber);
                    id = dataReader.ReadUInt64();
                    Debug.Assert(idNumber == id);

                    //dataWriter.Write(venueIdNumber);
                    venueId = dataReader.ReadUInt64();

                    //dataWriter.Write(publication_year);
                    year = dataReader.ReadUInt16();

                    //dataWriter.Write(titleLength);
                    UInt16 titleLength = dataReader.ReadUInt16();

                    //dataWriter.Write(numConcepts);
                    UInt16 numConcepts = dataReader.ReadUInt16();

                    //dataWriter.Write(numReferences);
                    UInt16 numReferences = dataReader.ReadUInt16();

                    //dataWriter.Write(numAuthorships);
                    UInt16 numAuthorships = dataReader.ReadUInt16();

                    //dataWriter.Write(titleBytes);
                    Byte[] titleBytes = new Byte[titleLength];
                    dataReader.Read(titleBytes);
                    title = Encoding.UTF8.GetString(titleBytes);

                    for (UInt16 idxConcept = 0; idxConcept < numConcepts; idxConcept++)
                    {
                        /*UInt64 conceptIdNumber = concepts[idxConcept].GetIdNumber();
                        double score = concepts[idxConcept].score;
                        dataWriter.Write(conceptIdNumber);
                        dataWriter.Write(score);*/
                        UInt64 conceptId = dataReader.ReadUInt64();
                        double score = dataReader.ReadDouble();
                        concepts.Add((conceptId, score));
                    }
                    for (UInt16 idxReference = 0; idxReference < numReferences; idxReference++)
                    {
                        /*UInt64 referenceIdNumber = GetIdNumber(referenced_works[idxReference]);
                        dataWriter.Write(referenceIdNumber);*/
                        UInt64 referenceId = dataReader.ReadUInt64();
                        references.Add(referenceId);
                    }
                    for (UInt16 idxAuthorship = 0; idxAuthorship < numAuthorships; idxAuthorship++)
                    {
                        /*UInt64 authorIdNumber = authorships[idxAuthorship].author.GetIdNumber();
                        UInt16 positionNumber = authorships[idxAuthorship].GetPositionNumber();
                        dataWriter.Write(authorIdNumber);
                        dataWriter.Write(positionNumber);
                        authorships[idxAuthorship].author.UpdateAuthors(authors);*/
                        UInt64 authorId = dataReader.ReadUInt64();
                        UInt16 positionNumber = dataReader.ReadUInt16();
                        authorships.Add((authorId, positionNumber));
                    }
                }
            }
        }

        public WorkEntity(string fileName, UInt64 idNumber, Int64 workPos)
        {
            Deserialize(fileName, idNumber, workPos);
        }

        Byte[] GetTitleBytes()
        {
            return Encoding.UTF8.GetBytes(title);
        }

        public void Serialize(FileStream indexFile, FileStream dataFile)
        {
            Int64 workPos = dataFile.Position;
            Byte[] titleBytes = GetTitleBytes();
            UInt16 titleLength = Convert.ToUInt16(titleBytes.Length);
            UInt16 numConcepts = Convert.ToUInt16(concepts.Count);
            UInt16 numReferences = Convert.ToUInt16(references.Count);
            UInt16 numAuthorships = Convert.ToUInt16(authorships.Count);

            BinaryWriter indexWriter = new BinaryWriter(indexFile);
            indexWriter.Write(id);
            indexWriter.Write(workPos);

            BinaryWriter dataWriter = new BinaryWriter(dataFile);
            dataWriter.Write(id);
            dataWriter.Write(venueId);
            dataWriter.Write(year);
            dataWriter.Write(titleLength);
            dataWriter.Write(numConcepts);
            dataWriter.Write(numReferences);
            dataWriter.Write(numAuthorships);
            dataWriter.Write(titleBytes);
            for (UInt16 idxConcept = 0; idxConcept < numConcepts; idxConcept++)
            {
                (UInt64 conceptId, double score) = concepts[idxConcept];
                dataWriter.Write(conceptId);
                dataWriter.Write(score);
            }
            for (UInt16 idxReference = 0; idxReference < numReferences; idxReference++)
            {
                UInt64 referenceId = references[idxReference];
                dataWriter.Write(referenceId);
            }
            for (UInt16 idxAuthorship = 0; idxAuthorship < numAuthorships; idxAuthorship++)
            {
                (UInt64 authorId, UInt16 positionNumber) = authorships[idxAuthorship];
                dataWriter.Write(authorId);
                dataWriter.Write(positionNumber);
            }
        }

        public static List<(UInt64, Int64)> LoadIndices(string path, UInt64 idxBucket)
        {
            string indexFileName = Path.Combine(path, $"work-index-{idxBucket}.wjf");
            return LoadIndices(indexFileName);
        }

        static List<(UInt64, Int64)> LoadIndices(string fileName)
        {
            List<(UInt64, Int64)> indices = new List<(UInt64, Int64)>();
            using (FileStream fs = File.OpenRead(fileName))
            {
                Int64 fileSize = fs.Length;
                Int64 numIndices = fileSize / 16;
                using (BinaryReader br = new BinaryReader(fs))
                {
                    for (Int64 i = 0; i < numIndices; i++)
                    {
                        UInt64 idNumber = br.ReadUInt64();
                        Int64 workPos = br.ReadInt64();
                        indices.Add((idNumber, workPos));
                    }
                }
            }
            return indices;
        }

        static bool IsDataFileName(string fileName)
        {
            return (fileName.StartsWith("work-data-") && fileName.EndsWith(".wjf"));
        }

        static string GetIndexFileName(string dataFileName)
        {
            return dataFileName.Replace("data","index");
        }

        static void RearrangeBuckets(string dataFileName, string indexFileName, string pathOutput, UInt64 numBuckets)
        {
            (FileStream, FileStream)[] buckets = new (FileStream, FileStream)[numBuckets];
            for (int idxBucket = 0; idxBucket < buckets.Length; idxBucket++)
            {
                FileStream indexFile = new FileStream(Path.Combine(pathOutput, $"work-index-{idxBucket}.wjf"), FileMode.Append);
                FileStream dataFile = new FileStream(Path.Combine(pathOutput, $"work-data-{idxBucket}.wjf"), FileMode.Append);
                buckets[idxBucket] = (indexFile, dataFile);
            }
            List<(UInt64, Int64)> indices = LoadIndices(indexFileName);
            foreach ((UInt64 workId, Int64 workPos) in indices)
            {
                UInt64 idxBucket = workId % numBuckets;
                WorkEntity workEntity = new WorkEntity(dataFileName, workId, workPos);
                (FileStream indexOut, FileStream dataOut) = buckets[idxBucket];
                workEntity.Serialize(indexOut, dataOut);
            }
            for (int idxBucket = 0; idxBucket < buckets.Length; idxBucket++)
            {
                (FileStream indexFile, FileStream dataFile)  = buckets[idxBucket];
                indexFile.Close();
                dataFile.Close();
            }
        }

        public static void RearrangeBuckets(string inputPath, string outputPath, UInt64 numBuckets)
        {
            DirectoryInfo diPathIn = new DirectoryInfo(inputPath);
            foreach(FileInfo fileIn in diPathIn.GetFiles())
            {
                if (IsDataFileName(fileIn.Name))
                {
                    Console.WriteLine($"Rearranging {fileIn.Name}");
                    string dataFileName = fileIn.FullName;
                    string indexFileName = GetIndexFileName(dataFileName);
                    RearrangeBuckets(dataFileName, indexFileName, outputPath, numBuckets);
                }
            }
        }

        public static void SortIndices(string outputPath, UInt64 numBuckets)
        {
            for (UInt64 idxBucket = 0; idxBucket < numBuckets; idxBucket++)
            {
                Console.WriteLine($"sort indices of bucket {idxBucket}");
                string indexFileName = Path.Combine(outputPath, $"work-index-{idxBucket}.wjf");
                string sortedIndexFileName = Path.Combine(outputPath, $"work-sorted_index-{idxBucket}.wjf");
                List<(UInt64, Int64)> indices = LoadIndices(indexFileName);
                indices.Sort((x, y) => Math.Sign(Convert.ToInt64(x.Item1) - Convert.ToInt64(y.Item1)));
                using (FileStream fileOut = File.Create(sortedIndexFileName))
                {
                    using (BinaryWriter indexWriter = new BinaryWriter(fileOut))
                    {
                        for (int i = 0; i < indices.Count; i++)
                        {
                            indexWriter.Write(indices[i].Item1);
                            indexWriter.Write(indices[i].Item2);
                        }
                    }
                }
            }
        }
    }
}
