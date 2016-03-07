The original algorithm is based on https://github.com/spotify/annoy.

Now there are a number of differences from Annoy's algorithm:

* Each vector has a binary signature which is used for fast pruning with xor instead of using dot products
* Before indexing the data is compressed to 32 dimensions with the SVD. However, the final scores are based on the original vectors
* Hadamard matrices are used (so called structured random projections) which are faster
* The data can be stored as bytes, two bytes, floats (4 bytes), doubles (8 bytes)
* When multiple candidate results appear in multiple trees, this information is used for pruning
* Quantization (coming soon)
* The trees are stored very efficiently  only two bytes are stored for splitting information instead of a high-dimensional vector
