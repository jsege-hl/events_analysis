# Load packages -----------------------------------------------------------

library(tidyverse)
library(tm)
library(proxy)
library(dbscan)
library(openxlsx)
library(parallelDist)

# Set the memory limit for the session to max
memory.limit(size = 8000000)


# Set config variables ----------------------------------------------------

# Sites to include
sites <- c('HL', 'GRT')

# Fraction of the page path strings to use for clustering (0 - 1)
sampling <- 1

# Number of clusters to compute
test.k <- 5

# Distance method to use
dist.method <- 'cosine'


# Load data ---------------------------------------------------------------

if (!file.exists('data/raw_dat.RData')) {
  library(bigrquery)
  
  cat('No local data found, sourcing from BigQuery')
  
  # Authorize with BigQuery
  bq_auth(use_oob = TRUE)
  
  # Set project ID
  projectid = 'bigquery-1084'
  
  # Define the query to BigQuery
  sql <- 
    'SELECT
    *
  FROM
    `sandbox-214618.jsege.page_element_clicks_jan_2020`'
  
  # Load the results from BigQuery
  raw_dat <- query_exec(sql, projectid, use_legacy_sql = FALSE, max_pages = Inf)
  
  # Save the query results for future analysis
  if (!dir.exists('data')) dir.create('data')
  
  save(raw_dat, file = 'raw_dat.RData')
} else {
  cat(paste('Loading local data sourced', file.info('data/raw_dat.RData')$ctime))
  
  load('data/raw_dat.RData')
}


# Filter and group data ---------------------------------------------------

# Get aggregate traffic data per pagepath and site
traffic <- raw_dat %>%
  mutate(Site = factor(toupper(Site)),
         Supergroup = forcats::fct_explicit_na(Supergroup)) %>%
  group_by(Date, Platform, PagePath, Site) %>%
  summarise(TotalSessions = mean(TotalSessions, na.rm = TRUE),
            TotalPageViews = mean(TotalPageViews, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE)) %>%
  group_by(PagePath, Site) %>%
  summarise(TotalCombinedSessions = sum(TotalSessions, na.rm = TRUE),
            TotalCombinedPageViews = sum(TotalPageViews, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE))

# Perform "report level filtering"
dat <- raw_dat %>%
  mutate(Site = factor(toupper(Site)),
         Supergroup = forcats::fct_explicit_na(Supergroup)) %>%
  left_join(traffic, by = c('PagePath', 'Site')) %>%
  filter(
    TotalCombinedPageViews >= 1000, # Limiting to observations with > 1000 monthly total pageviews
    PagePath != '/search', # Excluding search pages
    Site %in% sites
  )


# Prep data ---------------------------------------------------------------

# Sample the data
sampled_dat <- dat %>%
  sample_frac(sampling)

# Keep only unique page paths
paths <- unique(sampled_dat$PagePath)

#Split the page paths by path separators
path_split <- strsplit(paths, split = '/')

# Discard the first two elements, as these should be a blank and a category label
# TODO: evaluate if this makes sense
splits <- lapply(path_split, FUN = function(x) {
  return(x[3:length(x)])
})

strings <- sapply(splits, FUN = function(x, split) {
  s <- strsplit(x, split = split)
  
  j <- paste(unlist(s), collapse = ' ')
  
  return (j)
}, split = '-')


# Construct and prep the corpus -------------------------------------------

# If there is saved data for this sampling/k combo, load it, otherwise, compute
prep_file = paste0('data/clustering_prep_',
                   paste(sites, collapse = '_'),
                   '_S', gsub('[.]', '_', as.character(sampling)),
                   '_k', test.k,
                   '_', dist.method,
                   '.RData')

if (!file.exists(prep_file)) {
  cat('Prepping strings for clustering')
  
  # Create a corpus of paths
  corpus <- Corpus(VectorSource(strings))
  
  # Clean the corpus
  corpus.cleaned <- corpus %>%
    tm_map(removeWords, stopwords('english')) %>%
    tm_map(stemDocument, language = 'english') %>%
    tm_map(stripWhitespace)
  
  # Create the document-term matrix and remove empty documents
  dtm <- DocumentTermMatrix(corpus.cleaned)
  
  # Weight the document-term matrix by frequency and remove sparse terms
  dtm.tfidf <- weightTfIdf(dtm) %>%
    removeSparseTerms(0.999)
  
  # Convert weighted TFIDF to matrix and get auto-distance matrix
  tfidf.matrix <- as.matrix(dtm.tfidf)
  dist.matrix <- proxy::dist(tfidf.matrix, method = dist.method)
  
  # Since these processing steps take a lot of time, save for future use
  save(paths,
       strings,
       dtm,
       dtm.tfidf,
       tfidf.matrix,
       dist.matrix,
       file = prep_file)
} else {
  cat(paste('Loading local prep data computed', file.info(prep_file)$ctime))
  
  load(prep_file)
}


# Compute clusters and prep output ----------------------------------------


get_cluster_themes <- function(x, cluster_type) {
  # Get the subset of terms for the cluster
  subset <- which(res_fr[,cluster_type] == x)
  
  # Create a corpus from the clustered subset
  corpus.subset <- Corpus(VectorSource(strings[subset]))
  
  # Clean the corpus
  corpus.subset.cleaned <- corpus.subset %>%
    tm_map(removeWords, stopwords('english')) %>%
    tm_map(stemDocument, language = 'english') %>%
    tm_map(stripWhitespace)
  
  # Create the document-term matrix
  dtm.subset <- DocumentTermMatrix(corpus.subset.cleaned)
  
  # Find the most frequent terms associated with the cluster subset
  findMostFreqTerms(dtm.subset, 3, INDEX = rep(1, length(subset)))
}

# If there is saved data for this sampling/k combo, load it, otherwise, compute
res_file = paste0('data/clustering_results_',
                  paste(sites, collapse = '_'),
                  '_S', gsub('[.]', '_', as.character(sampling)),
                  '_k', test.k,
                  '_', dist.method)

if (!file.exists(paste0(res_file, '.RData'))) {
  # Perform three different clustering methods
  clustering.kmeans <- kmeans(tfidf.matrix, test.k)
  # clustering.hierarchical <- hclust(dist.matrix, method = 'ward.D2')
  clustering.dbscan <- dbscan::hdbscan(dist.matrix, minPts = 10)
  
  # Of the three clustering methods, choose one to be the primary
  master.cluster <- clustering.kmeans$cluster
  
  # # Project the distance matrix into lower-dimensional space for analysis
  # points <- cmdscale(dist.matrix, k = 3)
  # 
  # # Combine results into a frame
  # res_fr <- as.data.frame(points) %>%
  #   mutate(keywords = strings,
  #          PagePath = paths,
  #          k.cluster = clustering.kmeans$cluster,
  #          d.cluster = clustering.dbscan$cluster)
  
  res_fr <- data.frame(keywords = strings,
                       PagePath = paths,
                       k.cluster = clustering.kmeans$cluster,
                       d.cluster = clustering.dbscan$cluster)
  
  # Get the most common terms for each cluster
  k.themes <- lapply(X = unique(master.cluster),
                     FUN = get_cluster_themes, cluster_type = 'k.cluster')
  
  # Join the results of the themes analysis to concatenated strings
  k.themes.joined <- sapply(k.themes, FUN = function(x) {
    return(paste(names(x[[1]]), collapse = ', '))
  })
  
  # Associate the themes with their clusters and reorder them in cluster name order
  names(k.themes.joined) <- unique(master.cluster)
  k.themes.joined <- k.themes.joined[order(names(k.themes.joined))]
  
  # Get the most common terms for each cluster
  d.themes <- lapply(X = unique(clustering.dbscan$cluster),
                     FUN = get_cluster_themes, cluster_type = 'd.cluster')
  
  # Join the results of the themes analysis to concatenated strings
  d.themes.joined <- sapply(d.themes, FUN = function(x) {
    return(paste(names(x[[1]]), collapse = ', '))
  })
  
  # Associate the themes with their clusters and reorder them in cluster name order
  names(d.themes.joined) <- unique(clustering.dbscan$cluster) + 1
  d.themes.joined <- d.themes.joined[order(names(d.themes.joined))]
  
  # Add the themes strings into the rsults frame  
  res_fr <- res_fr %>%
    mutate(k.cluster.themes = k.themes.joined[res_fr$k.cluster],
           d.cluster.themes = d.themes.joined[res_fr$d.cluster + 1])
  
  # Write the results to Excel
  write.xlsx(x = res_fr, file = paste0(res_file, '.xlsx'), asTable = TRUE)
  
  # Since these processing steps take a lot of time, save for future use
  save(clustering.kmeans,
       # clustering.hierarchical,
       clustering.dbscan,
       # points,
       res_fr,
       file = paste0(res_file, '.RData'))
} else {
  cat(paste('Loading local results data computed', file.info(prep_file)$ctime))
  
  load(paste0(res_file, '.RData'))
}
