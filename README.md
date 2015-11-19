# random-projections-at-berlinbuzzwords

A library for approximate nearest neighbor search. This library can be used to:

* search words represented as word vectors
* search documents using a dense representation from Latent Dirichlet Allocation (LDA) and SVD
* build an image search engine
* build a music search engine
* build a recommendation system
* search representations of deep neural networks

The library was presented at BerlinBuzzwords 2015 in the following talk:

https://www.youtube.com/watch?v=V9zl09w1SGM&list=PLq-odUc2x7i-_qWWixXHZ6w-MxyLxEC7s&index=22

Technical information is available at:

http://stefansavev.com/randomtrees/

This library implements a fast and scalable fuzzy search of dense vectors (as opposed to exact search of sparse vectors) using the cosine similarity
 between the vectors. The library improves the speed of search up to 10 times compared to the
obvious brute force approach. With this library it is practical to compute all nearest neighbors
for all data points in datasets of sizes up to half a million data points.

| Dataset name           | Number of data points |  Number of dimensions | Number of Trees (Memory) Used | Queries per sec. | Queries per sec. brute force | Recall@10|
| ---------------------- | --------------------: | ---------------------:| -----------------------------:| ----------------:|-----------------------------:|---------:|
| MNIST                  | 42 000                 |  100                 | 10                            | 1104             | 164                          | 91.5%    |
| Google Word Vectors    | 70 000                 |  200                 | 50                            | 528              | 49                           | 91.0%    |
| Glove Word Vectors     | 400 000                |  100                 | 150                           | 165              | 18                           | 90.9%    |

90% Recall@10 means that in top 10 results returned by the library we could not find (100 - 90)% = 10%. This is common for search using
dense vectors. The remaining 10% can be found by increasing the number of trees, essentially giving more computational time and memory to the library.

##API

###Indexing (in batch mode)

```java
int dataDimension = 100;
int numTrees = 10;

//create an indexer
FuzzySearchIndexBuilder indexBuilder =
    new FuzzySearchIndexBuilder(dataDimension, FuzzySearchEngines.fastTrees(numTrees));

//add sample data
String key = "key";
int label = 8; //class/label in machine learning
double[] vector = ...; //a double array of dimension specified above
FuzzySearchItem item = new FuzzySearchItem(key, values);
indexBuilder.addItem(item);

//build the index
FuzzySearchIndex index = indexBuilder.build();

//save the index to file
index.save(outputIndexFile);
```

###Queries (Search)

```java
//load the index
FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);

//specify a query
double[] query = ...; //the query is a vector of dimension specified during indexing
//retrieve the results (data point names and similarity scores)
List<FuzzySearchResult> results = index.search(10, query); //return top 10 results
FuzzySearchResult topResult = results.get(0);
String topResultName = topResult.getName();
double topResultSimilarity = topResult.getCosineSimilarity();
```

#Usage

There is a sample projection using this library at https://github.com/stefansavev/fuzzysearch-demo