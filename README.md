# random-projections-at-berlinbuzzwords

A library for approximate nearest neighbor search. This library can be used to:

* efficiently search words represented as word vectors
* efficiently search documents using a dense representation from Latent Dirichlet Allocation (LDA) and SVD
* efficiently search representations of deep neural networks
* as a component of a recommendation system
* as a component of image search engine
* as a component of a music search engine


The library was presented at BerlinBuzzwords 2015 in the following talk:

https://www.youtube.com/watch?v=V9zl09w1SGM&list=PLq-odUc2x7i-_qWWixXHZ6w-MxyLxEC7s&index=22

Technical information is available at:

http://stefansavev.com/randomtrees/

This library implements a fast and scalable fuzzy search of dense vectors (as opposed to exact search of sparse vectors) using the cosine similarity
 between the vectors. The library improves the speed of search up to 10 times compared to the
obvious brute force approach. With this library it is practical to compute all nearest neighbors
for all data points in datasets of sizes up to half a million data points.

| Dataset name               | Number of data points |  Number of dimensions | Number of Trees (Memory) Used | Queries per sec. | Queries per sec. brute force | Recall@10|
| -------------------------- | --------------------: | ---------------------:| -----------------------------:| ----------------:|-----------------------------:|---------:|
| MNIST                      | 42 000                |  100                  | 10                            | 1104             | 164                          | 91.5%    |
| Google Word Vectors        | 70 000                |  200                  | 50                            | 528              | 49                           | 91.0%    |
| Glove Word Vectors         | 400 000               |  100                  | 150                           | 165              | 18                           | 90.9%    |
| Wikipedia Document Vectors | 4 000 000             |  128                  | 50                            | 268              | NA                           | 85.6%    |

90% Recall@10 means that in top 10 results returned by the library we found 9 (we failed to find 1). This is common for approximate search using
dense vectors. The remaining 1 result can be found by increasing the number of trees, essentially giving more computational time and memory to the library.

#Usage

There is a sample projection using this library at https://github.com/stefansavev/fuzzysearch-demo

# Feature Roadmap

* quantization: detect vectors which are very close (at similarity < 0.8) at indexing time. This also solves duplicate detection problem.
* bag of features: allow one object to be represented by multiple vectors
* query likelihood similarity: this makes sense when an object has multiple parts each represented by its own vector.
* integration with ElasticSearch: index and search dense vectors in ElasticSearch together with other fields
* reduced index size
* integration with learning algorithms. If your data is labeled, you can levarage the labels to improved your search
* integration with vizualization algorithms. Big high dimensional data can be successfully vizualized with the t-SNA algorithm
* 20-questions. How do you search if you don't know where to start. We implement the 20-questions algorithm to
help you search the data without knowing how to express your search.
* integrate multiple filters such as time and location
* integrate with sparse data
* integrate with complex relational data
* leverage external knowledge sources:
** For image data we levarage the representation from neural networks on image net
** For text data we levarage word vectors derived both on single words and phrases

#API

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

