When I rip out composite, I should replace it with std::vector
Used for batch reads and batch deletes

Is it necessary to have vector<KeySegmentPair> ?
Or is just doing the vector<VariantKey> ones sufficient?

This also means that the grouping might be important to keep, but that is easy to implement without copying

Also - the keys are likely to all be of the same type??

How to do the grouping?
  std::reference_wrapper ?

