# Load packages -----------------------------------------------------------

library(tidyverse)
library(plotly)
library(grid)
library(gridExtra)
library(scales)
library(viridis)
library(data.table)


# Set config variables ----------------------------------------------------

# Sites to include
sites <- c('HL', 'MNT', 'GRT')

# Platforms to include
platforms <- c('Mobile', 'Desktop', 'Tablet')

# Supergroups to include
supergroups <- c('Article Links', 'Bottom Page Recommendation', 'Read Next Right Rail', 'Other Right Rail')

# Text cluster file
cluster_file <- 'data/clustering_results_HL_GRT_S1_k4_cosine.RData'

# Minimum number of monthly pageviews to include
pageview_limit <- 1000

# Metric (CPPV or CPOPV)
metric <- 'CPPV'

# x axis for the bar chart
bar_x <- 'Platform'

# fill for the bar chart
bar_fill <- 'Supergroup'

# Bar background color
bg_color <- '#E0EDEF'


# Load data ---------------------------------------------------------------

if (!file.exists('data/raw_dat.RData')) {
  library(bigrquery)
  
  cat('No local data found, sourcing from BigQuery\n')
  
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
  save(raw_dat, 'raw_dat.RData')
} else {
  cat(paste('Loading local data sourced', file.info('data/raw_dat.RData')$ctime, '\n'))
  load('data/raw_dat.RData')
}

# Load Healthline's branded color palette
load('globals/hl_palette.RData')
palette_vis <- ggplot(data = data.frame(x = seq(1, length(hl_palette)), y = rep(1, length(hl_palette))), mapping = aes(x = x, y = y, fill = factor(x)), colour = NULL) + geom_bar(stat = 'identity') + scale_fill_manual(values = hl_palette)
hl_palette_darker <- hl_palette[c(8, 14, 15, 20, 1, 7, 13)]


# Filter and group data ---------------------------------------------------

# Function to return numbers as a string in a short format (e.g 1.2 K or 1.2 M)
num_in_km <- function(x) {
  if (is.na(x)) return(NA)
  
  if (x > 1000000) {
    return(sprintf('%.1f M',x/1000000))
  } else if (x > 1000) {
    return(sprintf('%.1f K', x/1000))
  } else {
    return(sprintf('%s', x))
  }
}

# Convert the grouping/join columns to factors and arrange them
raw_dat <- raw_dat %>%
  filter(toupper(Site) %in% sites,
         str_to_title(Platform) %in% platforms,
         Supergroup %in% supergroups) %>%
  mutate(Site = factor(toupper(Site), levels = sites),
         Platform = factor(str_to_title(Platform), levels = platforms),
         Supergroup = forcats::fct_explicit_na(Supergroup))

# Aggregate traffic per pagepath and site over the entire date range
page_traffic <- raw_dat %>%
  group_by(Date, Platform, PagePath, Site) %>%
  summarise(TotalSessions = first(TotalSessions),
            TotalPageViews = first(TotalPageViews),
            AvgSessionDuration = first(AvgSessionDuration)) %>%
  group_by(PagePath, Site) %>%
  summarise(TotalCombinedSessions = sum(TotalSessions, na.rm = TRUE),
            TotalCombinedPageViews = sum(TotalPageViews, na.rm = TRUE),
            TotalAvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE))

# Perform "report level filtering"
dat <- raw_dat %>%
  left_join(page_traffic, by = c('PagePath', 'Site')) %>%
  dplyr::filter(
    TotalCombinedPageViews >= pageview_limit, # Limiting to pages with at least the pageview_limit
    !(PagePath %in% c('/search', '/translate_c', '/', '')) # Excluding search and translate pages
  )

# Compute approximate number of distinct EventAction types per page path and event supergroup
# to get a rough number of clickable elements of each type per page per day
dates_actions_pagepaths <- dat %>%
  dplyr::filter(Supergroup == 'Article Links') %>%
  group_by(Site, PagePath, Supergroup, Date) %>%
  summarise(DistinctActions = n_distinct(EventAction)) %>%
  union(dat %>%
          dplyr::filter(!(Supergroup %in% c('Article Links', '(Missing)'))) %>%
          group_by(Site, PagePath, Supergroup, Date) %>%
          summarise(DistinctActions = n_distinct(EventLabel)))

# Aggregate daily clickable elements counts up to the site, pagepath, supergroup level
actions_pagepaths <- dates_actions_pagepaths %>%
  group_by(Site, PagePath, Supergroup) %>%
  summarise(DistinctActions = round(median(DistinctActions)))

# Pull page path clusters into the dataset
tenv <- new.env()
load(cluster_file, tenv)
res_fr <- get('res_fr', tenv) %>%
  mutate(PagePath = as.character(PagePath))
rm(tenv)

# Get aggregate event data by aggregating to date, platform, site, and supergroup
# Calculate aggregate measures including CPPV
grouped <- dat %>%
  group_by(Date, Platform, PagePath, Site, Supergroup) %>%
  summarise(TotalClicks = sum(TotalClicks, na.rm = TRUE),
            TotalSessions = first(TotalSessions),
            TotalPageViews = first(TotalPageViews),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE)) %>%
  left_join(res_fr, by = 'PagePath') %>%
  left_join(dates_actions_pagepaths, by = c('Site', 'PagePath', 'Supergroup', 'Date')) %>%
  mutate(ClicksPerOpp = TotalClicks / DistinctActions,
         PageOpps = DistinctActions * TotalPageViews,
         CPPV = 100 * TotalClicks / TotalPageViews,
         CPOPV = 100 * ClicksPerOpp / TotalPageViews)

# Make a data table version of grouped for quicker filtering later
grouped.dt <- data.table(grouped)
grouped.dt$metric <- grouped.dt[, ..metric]

# Get aggregate traffic data per platform and site
pf_traffic <- grouped %>%
  group_by(Date, Platform, PagePath, Site) %>%
  summarise(TotalSessions = first(TotalSessions),
            TotalPageViews = first(TotalPageViews),
            TotalPageOpps = mean(TotalPageViews * DistinctActions, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE)) %>%
  group_by(Platform, Site) %>%
  summarise(TotalSessions = sum(TotalSessions, na.rm = TRUE),
            TotalPageViews = sum(TotalPageViews, na.rm = TRUE),
            TotalPageOpps = sum(TotalPageOpps, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE))

# Get aggregate traffic data per site
traffic <- grouped %>%
  group_by(Date, Platform, PagePath, Site) %>%
  summarise(TotalSessions = first(TotalSessions),
            TotalPageViews = first(TotalPageViews),
            TotalPageOpps = mean(TotalPageViews * DistinctActions, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE)) %>%
  group_by(Site) %>%
  summarise(TotalSessions = sum(TotalSessions, na.rm = TRUE),
            TotalPageViews = sum(TotalPageViews, na.rm = TRUE),
            TotalPageOpps = sum(TotalPageOpps, na.rm = TRUE),
            AvgSessionDuration = mean(AvgSessionDuration, na.rm = TRUE))

# Group the event data by platform, supergroup, and site (summarise over all dates
# and page paths per platform)
pf_grouped <- grouped %>%
  group_by(Platform, Supergroup, Site) %>%
  summarise(TotalClicks = sum(TotalClicks, na.rm = TRUE),
            TotalClicksPerOpp = sum(ClicksPerOpp, na.rm = TRUE),
            AvgDistinctActions = ceiling(mean(DistinctActions, na.rm = TRUE)),
            CPPVMean = mean(CPPV),
            CPPVVar = var(CPPV),
            CPPVn = n(),
            CPOPVMean = mean(CPOPV),
            CPOPVVar = var(CPOPV),
            CPOPVn = n()) %>%
  left_join(pf_traffic, by = c('Platform', 'Site')) %>%
  mutate(CPPV = 100 * TotalClicks/TotalPageViews,
         CPOPV = 100 * TotalClicksPerOpp/TotalPageViews)

pf_grouped.dt <- data.table(pf_grouped)
pf_grouped.dt <- pf_grouped.dt[Supergroup != '(Missing)']
pf_grouped.dt$Supergroup <- factor(pf_grouped.dt$Supergroup)

# Group the event data by supergroup and site (summarise over all dates
# and page paths per platform)
site_grouped <- grouped %>%
  group_by(Supergroup, Site) %>%
  summarise(TotalClicks = sum(TotalClicks, na.rm = TRUE),
            TotalClicksPerOpp = sum(ClicksPerOpp, na.rm = TRUE),
            AvgDistinctActions = ceiling(mean(DistinctActions, na.rm = TRUE)),
            CPPVMean = mean(CPPV),
            CPPVVar = var(CPPV),
            CPPVn = n(),
            CPOPVMean = mean(CPOPV),
            CPOPVVar = var(CPOPV),
            CPOPVn = n()) %>%
  left_join(traffic, by = c('Site')) %>%
  mutate(CPPV = 100 * TotalClicks/TotalPageViews,
         CPOPV = 100 * TotalClicksPerOpp/TotalPageViews)

site_grouped.dt <- data.table(site_grouped)
site_grouped.dt <- site_grouped.dt[Supergroup != '(Missing)']
site_grouped.dt$Supergroup <- factor(site_grouped.dt$Supergroup)

# Group by path and supergroup and convert to datatable for quick filtering
path_event_grouped <- grouped %>%
  group_by(PagePath, Site, Supergroup) %>%
  summarise(TotalClicks = sum(TotalClicks, na.rm = TRUE),
            TotalClicksPerOpp = sum(ClicksPerOpp, na.rm = TRUE)) %>%
  left_join(actions_pagepaths, by = c('Site', 'PagePath', 'Supergroup')) %>%
  left_join(page_traffic, by = c('Site', 'PagePath')) %>%
  left_join(res_fr, by = 'PagePath') %>%
  mutate(CPPV = 100 * TotalClicks/TotalCombinedPageViews,
         CPOPV = 100 * TotalClicksPerOpp/TotalCombinedPageViews,
         CPOPV2 = 100 * TotalClicks/(TotalCombinedPageViews * DistinctActions)) %>%
  data.table

path_event_grouped <- path_event_grouped[!(Supergroup %in% c('None', '(Missing)'))]
path_event_grouped$Supergroup <- factor(path_event_grouped$Supergroup)

# Group by supergroup only
overall_grouped = site_grouped %>%
  group_by(Supergroup) %>%
  summarise(TotalClicks = sum(TotalClicks),
            TotalClicksPerOpp = sum(TotalClicksPerOpp),
            AvgDistinctActions = ceiling(mean(AvgDistinctActions)),
            TotalSessions = sum(TotalSessions),
            TotalPageViews = sum(TotalPageViews)) %>%
  mutate(CPPV = sprintf('%.1f%%', 100 *TotalClicks/TotalPageViews)) %>%
  mutate_if(is.numeric, sapply, FUN = num_in_km)

# Get top 20 page paths by CPPV and CPOPV
top_20_CPPV <- path_event_grouped %>%
  top_n(20, wt = CPPV) %>%
  arrange(desc(CPPV))

top_20_CPOPV <- path_event_grouped %>%
  top_n(20, wt = CPOPV) %>%
  arrange(desc(CPOPV))

# Get the date range covered by the filtered and grouped data
date_range <- unique(range(as.Date(grouped$Date)))
date_range_string <- sprintf('(%s)', paste(date_range, collapse = ' - '))


# Explore DistinctActions/CPPV Relationship -------------------------------

for (s in append(c('All'), levels(grouped$Supergroup)[levels(grouped$Supergroup) != '(Missing)'])) {
  if (s == 'All') plot_dat = path_event_grouped else plot_dat <- path_event_grouped[Supergroup == s]
  
  h <- ggplot(data = plot_dat,
              mapping = aes(x = DistinctActions,
                            y = CPPV)) +
    geom_hex(aes(fill="#000000",alpha=log(..count..)),fill=hl_palette_darker[1]) +
    theme_bw() +
    labs(title = 'Hexbin of Page Counts',
         subtitle = paste('By Distinct Click Destinations and CPPV - ', s),
         x = 'Distinct Element Destinations',
         y = if (metric == 'CPPV') 'Clicks per 100 Pageviews' else if (metric == 'CPOPV') 'Normalized Clicks per 100 Pageviews' else metric,
         alpha = 'Log Page Count')
  
  png(sprintf('plots/CPPV_v_distinct_element_destinations_%s.png', s), width = 800, height = 800, res = 100)
  print(h)
  dev.off()
}

mod <- lm(formula = CPPV~DistinctActions, data = path_event_grouped)


# Probability distributions -----------------------------------------------

# Probability distribution by site and supergroup
site <- 'GRT'

dens_dat <- grouped.dt %>%
  filter(Supergroup != '(Missing)',
         Site == site) %>%
  mutate(Supergroup = factor(Supergroup, levels = c('Other Right Rail', 'Read Next Right Rail', 'Article Links', 'Bottom Page Recommendation')))

colors = hl_palette_darker[c(3, 4, 1, 2)]
names(colors) <- c('Other Right Rail', 'Read Next Right Rail', 'Article Links', 'Bottom Page Recommendation')

dens <- ggplot(data = dens_dat,
               mapping = aes(x = CPPV, fill = Supergroup, colour = Supergroup)) +
  geom_density(alpha = 0.5) +
  scale_x_continuous(limits = c(0, 50), name = metric) +
  scale_y_continuous(name = 'Density') +
  scale_fill_manual(values = colors) +
  scale_colour_manual(values = colors) +
  theme(axis.text.x = element_text(size = 12),
        axis.text.y = element_text(size = 12),
        axis.title.x = element_text(size = 12, face = 'bold'),
        axis.title.y = element_text(size = 12, face = 'bold'),
        legend.title =element_blank(),
        legend.text = element_text(size = 12),
        panel.background = element_rect(fill = bg_color, colour = 'white'),
        plot.background = element_rect(fill = bg_color),
        legend.background = element_rect(fill = bg_color),
        strip.background = element_blank(),
        strip.text = element_text(size = 12, face = 'bold'),
        panel.grid = element_blank(),
        axis.line = element_line(colour = 'white'))

png(filename=sprintf('plots/CPOPV_densities_Supergroup_%s.png', site), width = 800, height =500, res = 100)
print(dens)
dev.off()

# Get approximate probabilities of 1+ clicks per pageview
probs <- sapply(supergroups, FUN = function(x) {
  if (length(dens_dat[dens_dat$Supergroup == x, metric]) == 0 | length(which(!is.na(dens_dat[dens_dat$Supergroup == x, metric]))) == 0) return(0)
  
  sprintf(
    '%.2f%%',
    100 - 100 * ecdf(as.numeric(dens_dat[dens_dat$Supergroup == x, metric]))(99.99)
  )
})

# Violin plot to show density of different click-per-pageview values by event category
violin_dat <- grouped.dt[Supergroup != '(Missing)']
violin_dat$Supergroup <- factor(violin_dat$Supergroup)

violin <- ggplot(data = violin_dat,
                 mapping = aes_string(x = paste0('reorder(Supergroup, ', metric, ')'), y = metric, fill = 'Supergroup')) +
  geom_violin(colour=NA) +
  facet_grid(cols = vars(Site)) +
  scale_y_continuous(limits = c(0, 100)) +
  scale_fill_manual(values = hl_palette_darker) +
  labs(title = 'Clickthrough Rates',
       subtitle = 'By Category and Density',
       x = 'Event Category',
       y = if (metric == 'CPPV') 'Clicks per 100 Pageviews' else if (metric == 'CPOPV') 'Normalized Clicks per 100 Pageviews' else metric) +
  theme(axis.text.x = element_text(angle = 90, size = 10, face = 'bold'),
        axis.title.x = element_blank(),
        axis.title.y = element_text(size = 10, face = 'bold'),
        panel.background = element_rect(fill = bg_color, colour = 'white'),
        plot.background = element_rect(fill = bg_color),
        legend.background = element_rect(fill = bg_color),
        panel.grid = element_blank(),
        axis.line = element_line(colour = 'white'),
        strip.background = element_blank(),
        strip.text = element_text(size = 10, face = 'bold'))

png(filename=sprintf('plots/violin_%s.png', metric), width = 1000, height = 800, res = 100)
print(violin)
dev.off()


# Bar charts --------------------------------------------------------------

# Function to generate and save the bar chart
generate_bar <- function(bar_x, metric, site, bar_fill) {
  bar_dat$click_lab <- sapply(bar_dat$TotalClicks, FUN = num_in_km)
  bar_dat$Supergroup <- factor(bar_dat$Supergroup, levels = supergroups)
  
  metric_adjust <- list('CPPV' = 1, 'CPOPV' = 0.08, 'AvgDistinctActions' = 0.7)
  metric_format <- list('CPPV' = '\'%.1f\'', 'CPOPV' = '\'%.1f\'', 'AvgDistinctActions' = '\'%s\'')
  
  # Bar chart showing average clicks-per-pageview
  bar <- ggplot(data = bar_dat, mapping = aes_string(x = bar_x,
                                                     y = metric,
                                                     fill=bar_fill,
                                                     colour=NULL,
                                                     group=bar_fill)) +
    geom_col(mapping = aes_string(customdata = 'TotalClicks'),
             position = position_dodge(width = 0.9),
             width = 0.8) +
    geom_text(mapping = aes_string(label = paste0('sprintf(', metric_format[metric], ', ', metric, ')'),
                                   y = paste0(metric, ' + ', metric_adjust[metric])),
              position = position_dodge(width = 0.88),
              vjust = 0,
              size = 5,
              fontface = 'bold') +
    # facet_grid(rows = vars(Site)) +
    scale_fill_manual(values = hl_palette_darker) +
    scale_x_discrete(name = bar_x) +
    scale_y_continuous(name = if (metric == 'CPPV') 'Clicks per 100 Pageviews' else if (metric == 'CPOPV') 'Normalized Clicks per 100 Pageviews' else if (metric == 'AvgDistinctActions') 'Avg. Available Options' else metric) +
    theme(axis.text.x = element_text(size = 12, face = 'bold'),
          axis.text.y = element_text(size = 12),
          axis.title.x = element_blank(),
          axis.title.y = element_text(size = 12, face = 'bold'),
          legend.title =element_blank(),
          legend.text = element_text(size = 12),
          panel.background = element_rect(fill = bg_color, colour = 'white'),
          plot.background = element_rect(fill = bg_color),
          legend.background = element_rect(fill = bg_color),
          strip.background = element_blank(),
          strip.text = element_text(size = 12, face = 'bold'),
          panel.grid = element_blank(),
          axis.line = element_line(colour = 'white')) +
    labs(fill = if (bar_fill == 'Supergroup') 'Click Element' else bar_fill)
  
  png(filename=sprintf('plots/%s_%s_%s.png', bar_x, metric, site), width = 800, height = 500, res = 100)
  print(bar)
  dev.off()
}

bar_x <- 'Site'
metric <- 'CPOPV'
site = 'GRT'
if (bar_x == 'Platform') {
  bar_dat <- pf_grouped.dt[Site == site]
  
  for (l in levels(bar_dat$Supergroup)) {
    for (p in levels(bar_dat$Platform)) {
      sub <- bar_dat[Platform == p]
      if (nrow(sub[Supergroup == l]) == 0) {
        sub_vect <- sub[nrow(sub)]
        sub_vect$Supergroup <- l
        for (c in names(sub_vect)[4:ncol(sub_vect)]) {
          sub_vect[, c] <- 0
        }
        cat('Appending missing data\n')
        
        bar_dat <- rbind(bar_dat, sub_vect)
      }
    }
  }
} else {
  bar_dat <- site_grouped.dt
  
  for (l in levels(bar_dat$Supergroup)) {
    for (s in levels(bar_dat$Site)) {
      sub <- bar_dat[Site == s]
      if (nrow(sub[Supergroup == l]) == 0) {
        sub_vect <- sub[nrow(sub)]
        sub_vect$Supergroup <- l
        for (c in names(sub_vect)[4:ncol(sub_vect)]) {
          sub_vect[, c] <- 0
        }
        cat('Appending missing data\n')
        
        bar_dat <- rbind(bar_dat, sub_vect)
      }
    }
  }
}

generate_bar(bar_x, metric, site, bar_fill)


# Link Analysis -----------------------------------------------------------

# Get aggregate event data by aggregating to date, platform, site, and supergroup
# Calculate aggregate measures including CPPV
links_grouped <- dat %>%
  filter(Supergroup == 'Article Links') %>%
  mutate(EventCategory = gsub('Article Body - ', '', EventCategory),
         ActDomain = grepl('act[.]', EventLabel)) %>%
  mutate(EventCategory = paste0(EventCategory, ifelse(ActDomain, ' - Act', ''))) %>%
  group_by(Site, EventCategory, Platform)  %>%
  summarise(TotalClicks = sum(TotalClicks, na.rm = TRUE)) %>%
  left_join(dat %>%
              filter(Supergroup == 'Article Links') %>%
              group_by(Site, Platform) %>%
              dplyr::summarise(SiteClicks = sum(TotalClicks)), by = c('Site', 'Platform')) %>%
  arrange(desc(Site), desc(Platform), desc(EventCategory)) %>%
  mutate(GroupPerc = 100 * TotalClicks / SiteClicks,
         ypos = cumsum(GroupPerc) - 0.5 * GroupPerc)

links_pies <- ggplot(data = links_grouped, mapping = aes(x = '', y = GroupPerc, fill = EventCategory)) +
  geom_bar(stat = 'identity')  +
  coord_polar('y', start = 0) +
  geom_text(mapping = aes(y = 0.2*pi*GroupPerc, label = sprintf('%.1f%%', GroupPerc)), colour = '#555555', fontface = 'bold') +
  facet_grid(cols = vars(Platform),
             rows = vars(Site)) +
  theme(axis.text.x = element_blank(),
        axis.title = element_blank(),
        axis.ticks = element_blank(),
        panel.grid = element_blank(),
        legend.title = element_blank(),
        legend.text = element_text(size = 10, face = 'bold'),
        panel.background = element_rect(fill = bg_color, colour = 'white'),
        plot.background = element_rect(fill = bg_color),
        legend.background = element_rect(fill = bg_color),
        strip.background = element_blank(),
        strip.text = element_text(size = 10, face = 'bold')) +
  scale_fill_manual(values = hl_palette_darker)
  # ggtitle('Link Destinations by Site and Platform')

png(filename='plots/link_pies_w_act.png', width = 1200, height = 800, res = 100)
print(links_pies)
dev.off()


# Hypothesis testing ------------------------------------------------------

# Subset the grouped data
subset <- grouped.dt[Site == 'HL' & grepl('Right', Supergroup)]
subset$Supergroup <- factor(subset$Supergroup)

metric <- 'CPOPV'

# Get a summary table FYI
summs <- subset %>%
  group_by(Supergroup) %>%
  dplyr::summarise(count = n(),
                   CPPVmean = mean(CPOPV, na.rm = TRUE),
                   CPPVsd = sd(CPOPV, na.rm = TRUE))

# Perform anova
form <- as.formula(paste(metric, '~ Platform'))
anova <- aov(form, data = subset)

# Print results
summary(anova)
TukeyHSD(anova)
