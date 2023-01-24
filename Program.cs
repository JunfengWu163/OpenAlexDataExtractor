using System;

const string inputPath = "E:\\OpenAlexUnsorted";
const string outputPath = "E:\\OpenAlex64";
OpenAlexDataExtractor.Work.ExtractUnsorted("F:\\openalex-snapshot\\data", inputPath, 16);
OpenAlexDataExtractor.WorkEntity.RearrangeBuckets(inputPath, outputPath, 64);
OpenAlexDataExtractor.WorkEntity.SortIndices(outputPath, 64);

Parallel.For(0, 64, idxBucket => {
    Console.WriteLine($"testing bucket {idxBucket}");

    List<(UInt64, Int64)> indices = OpenAlexDataExtractor.WorkEntity.LoadIndices(outputPath, Convert.ToUInt64(idxBucket));
    foreach ((UInt64, Int64) index in indices)
    {
        OpenAlexDataExtractor.WorkEntity workEntity = new OpenAlexDataExtractor.WorkEntity(index.Item1, 64, outputPath);
        if (workEntity.id != index.Item1)
        {
            Console.WriteLine($"Cannot found {index.Item1}");
        }
    }
});

OpenAlexDataExtractor.Concept.Extract("F:\\openalex-snapshot\\data", inputPath);
OpenAlexDataExtractor.ConceptEntity.SortIndices(outputPath);

{
    List<(UInt64, Int64)> indices = OpenAlexDataExtractor.ConceptEntity.LoadIndices(Path.Combine(outputPath,"concept-sorted_index.wjf"));
    foreach ((UInt64, Int64) index in indices)
    {
        OpenAlexDataExtractor.ConceptEntity conceptEntity = new OpenAlexDataExtractor.ConceptEntity(index.Item1, outputPath);
        if (conceptEntity.id != index.Item1)
        {
            Console.WriteLine($"Cannot found {index.Item1}");
        }
    }
}

OpenAlexDataExtractor.AuthorEntity.SortIndices(outputPath);

{
    List<(UInt64, Int64)> indices = OpenAlexDataExtractor.AuthorEntity.LoadIndices(Path.Combine(outputPath, "author-index.wjf"));
    foreach ((UInt64, Int64) index in indices)
    {
        OpenAlexDataExtractor.AuthorEntity authorEntity = new OpenAlexDataExtractor.AuthorEntity(index.Item1, outputPath);
        if (authorEntity.id != index.Item1)
        {
            Console.WriteLine($"Cannot found {index.Item1}");
        }
    }
}

OpenAlexDataExtractor.Venue.Extract("F:\\openalex-snapshot\\data", inputPath);
OpenAlexDataExtractor.VenueEntity.SortIndices(outputPath);

{
    List<(UInt64, Int64)> indices = OpenAlexDataExtractor.VenueEntity.LoadIndices(Path.Combine(outputPath, "venue-index.wjf"));
    foreach ((UInt64, Int64) index in indices)
    {
        OpenAlexDataExtractor.VenueEntity venueEntity = new OpenAlexDataExtractor.VenueEntity(index.Item1, outputPath);
        if (venueEntity.id != index.Item1)
        {
            Console.WriteLine($"Cannot found {index.Item1}");
        }
    }
}