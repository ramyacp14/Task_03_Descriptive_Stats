# Task_03_Descriptive_Stats

## Overview

This repository contains scripts and analysis for **Research Task 3: Descriptive Statistics with and without 3rd Party Libraries (Pandas/Polars)**.  
The goal is to produce descriptive statistics on three datasets related to the 2024 US Presidential election social media activity:

- Facebook Ads (`2024_fb_ads_president_scored_anon.csv`)
- Facebook Posts (`2024_fb_posts_president_scored_anon.csv`)
- Twitter Posts (`2024_tw_posts_president_scored_anon.csv`)

Using three approaches:  
1. Pure Python (standard library only)  
2. Pandas (popular data analysis library)  
3. Polars (high-performance DataFrame library)

---

## Repository Contents

- `pure_python_stats.py` — Descriptive statistics implemented using only Python standard library.  
- `pandas_stats.py` — Analysis using Pandas library.  
- `polars_stats.py` — Analysis using Polars library.  
- `polars_analysis_results.json` — Sample output JSON results from Polars script.  

---

## How to Run

### Prerequisites

- Python 3.8 or higher  
- For Pandas and Polars scripts, install dependencies using:  
  ```bash
  pip install pandas polars

## Findings and Comparison of Descriptive Statistics: Pure Python vs Pandas vs Polars

### 1. **Data Overview and Completeness**

* **Facebook Ads Dataset**

  * Rows: \~246,745
  * Columns: 41
  * Data completeness near 100%, only a small missing percentage in `bylines` (\~0.4%).
  * Key numeric columns such as `estimated_audience_size`, `estimated_impressions`, and `estimated_spend` show high variance and skew (e.g., median values much lower than mean).

* **Facebook Posts Dataset**

  * Rows: \~19,009
  * Columns: 56
  * Data completeness lower (\~88%), with several columns 100% missing (Sponsor info) and others heavily missing (e.g., Video Share Status \~82.8% missing).
  * Engagement metrics like Likes, Comments, Shares show highly skewed distributions (high standard deviation, max values far from medians).

* **Twitter Posts Dataset**

  * Rows: \~27,304
  * Columns: 47
  * Data completeness \~94%, some columns have very high missingness (e.g., `quoteId` and `inReplyToId` \~88% missing).
  * Numeric columns like retweetCount, replyCount, and likeCount also show skew with large max values relative to medians.

---

### 2. **Consistency of Results**

* All three approaches—pure Python, Pandas, and Polars—produce **consistent descriptive statistics** for count, mean, min, max, median, and standard deviation on numeric columns.
* Unique value counts and most frequent values for categorical columns match closely across methods.
* Aggregations by `page_id` and `(page_id, ad_id)` yield consistent insights such as average ads/posts per page and distribution of unique page-ad combinations.

---

### 3. **Performance**

| Approach        | Total Processing Time (approx.)        | Notes                                                                         |
| --------------- | -------------------------------------- | ----------------------------------------------------------------------------- |
| **Polars**      | \~1 second                             | Fastest; efficient memory use and parallelism benefit large datasets.         |
| **Pandas**      | \~1-2 seconds (estimated)              | Well-optimized for medium-sized data; familiar API for most data scientists.  |
| **Pure Python** | Several seconds to minutes (estimated) | Slowest; requires manual coding for many steps and careful memory management. |

* Polars outperforms Pandas significantly in speed on large datasets due to Rust-based engine and parallel processing.
* Pure Python’s performance suffers due to lack of vectorized operations and overhead of explicit loops.

---

### 4. **Ease of Implementation and Usability**

| Approach        | Ease of Use | Code Complexity | Suitability for Junior Analysts                                                             |
| --------------- | ----------- | --------------- | ------------------------------------------------------------------------------------------- |
| **Polars**      | Moderate    | Moderate        | Growing but less common than Pandas; syntax less familiar.                                  |
| **Pandas**      | Easy        | Low             | Industry standard; extensive documentation and community support.                           |
| **Pure Python** | Difficult   | High            | Educational value for understanding fundamentals but complex for large real-world datasets. |

* Pure Python requires manual parsing, aggregation, and handling of missing data, increasing chance of bugs.
* Pandas provides convenient descriptive functions (`describe()`, `value_counts()`) that dramatically reduce coding effort.
* Polars API is concise and very performant, but less widespread familiarity means a learning curve exists.

---

### 5. **Challenges Encountered**

* Handling missing values consistently across all approaches required explicit coding in pure Python, while Pandas and Polars have built-in support.
* Matching output formatting and naming conventions across libraries for easier comparison took some adjustments.
* Polars required installing additional dependencies but provided the best performance trade-off.
* Pure Python solution needed extra care in type conversions and performance optimization.

---

### 6. **Recommendations**

* **For quick, reliable analysis on moderately sized datasets:** Use **Pandas** for its balance of ease, power, and ecosystem support.
* **For large datasets requiring speed and efficiency:** Use **Polars**, especially when working with multi-core CPUs or memory constraints.
* **For educational purposes or environments with no third-party libraries allowed:** Use **Pure Python**, but be aware of the time and complexity involved.

---

### 7. **Narrative Insights**

* Social media engagement metrics (likes, shares, comments, retweets) are highly skewed, indicating a few viral posts or ads dominate interaction.
* The `page_id` distribution shows a small number of pages responsible for a large number of ads/posts, reflecting concentrated campaign efforts.
* Missing sponsor information in Facebook posts may limit some analyses but is consistently handled across all methods.
* These descriptive statistics set the foundation for deeper analysis like sentiment analysis, network influence, or campaign effectiveness.
