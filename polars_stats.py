import polars as pl
import json
import time
import ast

class PolarsAnalyzer:
    def __init__(self):
        self.my_data_files = {}

    def read_csv_with_polars(self, file_path):
        """Load CSV using Polars - supposedly faster than pandas!"""
        try:
            my_df = pl.read_csv(file_path, ignore_errors=True)
            print(f"Polars loaded: {my_df.height} rows, {my_df.width} columns")
            return my_df
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return pl.DataFrame()
        except Exception as error:
            print(f"Error reading file: {error}")
            return pl.DataFrame()

    def analyze_with_polars(self, df, dataset_name):
        """Analyze dataset using Polars methods"""
        if df.is_empty():
            return {'error': 'Empty dataset!'}

        start_time = time.time()

        # Basic dataset info
        dataset_basics = {
            'name': dataset_name,
            'num_rows': df.height,
            'num_cols': df.width,
            'memory_estimate': df.estimated_size(),
            'column_list': df.columns
        }

        # Examine each column
        column_analysis = {}

        for col_name in df.columns:
            col_data = df.select(col_name)

            # Basic column info
            col_info = {
                'column': col_name,
                'data_type': str(df[col_name].dtype),
                'total_rows': df.height,
                'non_null_rows': df[col_name].count(),
                'null_rows': df[col_name].null_count(),
                'percent_null': (df[col_name].null_count() / df.height) * 100
            }

            try:
                sample_value = df[col_name].drop_nulls().to_list()[0]
                if isinstance(sample_value, str):
                    try:
                        parsed_value = ast.literal_eval(sample_value)
                        if isinstance(parsed_value, dict):
                            col_info.update({
                                'category': 'dictionary',
                                'example_keys': list(parsed_value.keys())[:5],
                                'example_value': parsed_value[list(parsed_value.keys())[0]]
                            })
                            column_analysis[col_name] = col_info
                            continue
                    except:
                        pass
            except:
                pass

            # Check if it's numeric or text
            if df[col_name].dtype.is_numeric():
                # Numeric column - calculate stats
                try:
                    num_stats = df.select([
                        pl.col(col_name).mean().alias('avg'),
                        pl.col(col_name).std().alias('std_dev'),
                        pl.col(col_name).min().alias('minimum'),
                        pl.col(col_name).max().alias('maximum'),
                        pl.col(col_name).median().alias('middle'),
                        pl.col(col_name).quantile(0.25).alias('q1'),
                        pl.col(col_name).quantile(0.75).alias('q3')
                    ]).to_dicts()[0]

                    col_info.update({
                        'category': 'numbers',
                        **num_stats
                    })
                except:
                    col_info.update({
                        'category': 'numbers',
                        'error': 'Could not calculate stats'
                    })

            else:
                # Text/categorical column
                try:
                    freq_table = df[col_name].value_counts().sort('count', descending=True)
                    unique_vals = df[col_name].n_unique()

                    col_info.update({
                        'category': 'text',
                        'unique_count': unique_vals,
                        'top_value': freq_table[col_name][0] if freq_table.height > 0 else None,
                        'top_frequency': freq_table['count'][0] if freq_table.height > 0 else 0,
                        'top_5_frequencies': {
                            str(freq_table[col_name][i]): freq_table['count'][i] 
                            for i in range(min(5, freq_table.height))
                        }
                    })
                except Exception as e:
                    col_info.update({
                        'category': 'text',
                        'unique_count': 0,
                        'analysis_error': str(e)
                    })

            column_analysis[col_name] = col_info

        # Overall data quality
        total_possible_values = df.height * df.width
        actual_null_values = sum(df[col].null_count() for col in df.columns)

        data_quality = {
            'completeness_percent': ((total_possible_values - actual_null_values) / total_possible_values) * 100,
            'columns_with_nulls': sum(1 for col in df.columns if df[col].null_count() > 0),
            'total_null_count': actual_null_values
        }

        end_time = time.time()

        return {
            'dataset_basics': dataset_basics,
            'column_analysis': column_analysis,
            'data_quality': data_quality,
            'analysis_time': end_time - start_time
        }
    
    def group_analysis(self, df, grouping_cols):
        """Do groupby analysis with Polars"""
        if df.is_empty() or not grouping_cols:
            return {}
        
        try:
            # Check if columns exist
            missing_cols = [col for col in grouping_cols if col not in df.columns]
            if missing_cols:
                return {'error': f'Missing these columns: {missing_cols}'}
            
            # Group and get sizes
            group_sizes = df.group_by(grouping_cols).len().sort('len', descending=True)
            
            group_summary = {
                'total_groups': group_sizes.height,
                'smallest_group': group_sizes['len'].min(),
                'largest_group': group_sizes['len'].max(),
                'average_group_size': group_sizes['len'].mean(),
                'median_group_size': group_sizes['len'].median(),
                'sum_of_all_records': group_sizes['len'].sum()
            }
            
            # Analyze numeric columns within groups
            numeric_columns = [col for col in df.columns 
                             if df[col].dtype.is_numeric() and col not in grouping_cols]
            
            if numeric_columns:
                numeric_group_analysis = {}
                
                for num_col in numeric_columns:
                    try:
                        grouped_stats = df.group_by(grouping_cols).agg([
                            pl.col(num_col).count().alias('count'),
                            pl.col(num_col).mean().alias('mean'),
                            pl.col(num_col).std().alias('std'),
                            pl.col(num_col).min().alias('min'),
                            pl.col(num_col).max().alias('max')
                        ])
                        
                        numeric_group_analysis[num_col] = {
                            'overall_mean': grouped_stats['mean'].mean(),
                            'mean_variation': grouped_stats['mean'].std(),
                            'lowest_group_mean': grouped_stats['mean'].min(),
                            'highest_group_mean': grouped_stats['mean'].max(),
                            'groups_with_data': grouped_stats['count'].filter(pl.col('count') > 0).height
                        }
                    except Exception as e:
                        numeric_group_analysis[num_col] = {'error': str(e)}
                
                group_summary['numeric_analysis'] = numeric_group_analysis
            
            return group_summary
            
        except Exception as e:
            return {'error': f'Group analysis failed: {str(e)}'}
    
    def display_results(self, analysis):
        """Show analysis results nicely"""
        print(f"\n{'='*60}")
        print(f"POLARS ANALYSIS: {analysis['dataset_basics']['name']}")
        print(f"{'='*60}")
        print(f"Rows: {analysis['dataset_basics']['num_rows']:,}")
        print(f"Columns: {analysis['dataset_basics']['num_cols']}")
        print(f"Estimated memory: {analysis['dataset_basics']['memory_estimate']:,} bytes")
        print(f"Analysis time: {analysis['analysis_time']:.2f} seconds")
        print(f"Data completeness: {analysis['data_quality']['completeness_percent']:.1f}%")
        
        print(f"\n{'Column Summary':^60}")
        print("-" * 60)
        
        number_cols = []
        text_cols = []
        
        for col_name, info in analysis['column_analysis'].items():
            if info['category'] == 'numbers':
                number_cols.append(col_name)
            else:
                text_cols.append(col_name)
        
        print(f"Numeric columns: {len(number_cols)}")
        print(f"Text/categorical columns: {len(text_cols)}")
        
        # Show columns with missing values
        missing_info = [(col, info['percent_null']) 
                       for col, info in analysis['column_analysis'].items()
                       if info['percent_null'] > 0]
        missing_info.sort(key=lambda x: x[1], reverse=True)
        
        if missing_info:
            print(f"\nColumns with missing data:")
            for col, pct in missing_info[:5]:
                print(f"  {col}: {pct:.1f}% missing")
        else:
            print(f"\nNo missing data found!")
        
        # Show sample numeric columns
        if number_cols:
            print(f"\nSample numeric columns:")
            for col_name in number_cols[:3]:
                info = analysis['column_analysis'][col_name]
                if 'avg' in info:
                    print(f"\n{col_name}:")
                    print(f"  Average: {info['avg']:.2f}")
                    print(f"  Std Dev: {info['std_dev']:.2f}")
                    print(f"  Range: {info['minimum']} to {info['maximum']}")
                    print(f"  Median: {info['middle']:.2f}")
        
        # Show sample text columns
        if text_cols:
            print(f"\nSample text columns:")
            for col_name in text_cols[:3]:
                info = analysis['column_analysis'][col_name]
                print(f"\n{col_name}:")
                print(f"  Unique values: {info.get('unique_count', 'N/A'):,}")
                if 'top_value' in info and info['top_value'] is not None:
                    print(f"  Most common: {info['top_value']} ({info['top_frequency']} times)")

def run_polars_analysis():
    """Main function to run the Polars analysis"""
    analyzer = PolarsAnalyzer()
    
    # Files to analyze
    data_files = {
        'facebook_ads': '2024_fb_ads_president_scored_anon.csv',
        'facebook_posts': '2024_fb_posts_president_scored_anon.csv',
        'twitter_posts': '2024_tw_posts_president_scored_anon.csv'
    }
    
    all_results = {}
    
    for dataset_name, file_path in data_files.items():
        print(f"\n{'='*60}")
        print(f"Loading {file_path} with Polars...")
        print(f"{'='*60}")
        
        # Load the dataset
        df = analyzer.read_csv_with_polars(file_path)
        
        if not df.is_empty():
            # Analyze the dataset
            analysis = analyzer.analyze_with_polars(df, dataset_name)
            
            if 'error' not in analysis:
                all_results[dataset_name] = analysis
                analyzer.display_results(analysis)
                
                # Special groupby analysis for Facebook ads
                if dataset_name == 'facebook_ads' and 'page_id' in df.columns:
                    print(f"\n{'Grouping Facebook ads by page_id':^60}")
                    print("-" * 60)
                    page_groups = analyzer.group_analysis(df, ['page_id'])
                    
                    if 'error' not in page_groups:
                        print(f"Total unique pages: {page_groups['total_groups']}")
                        print(f"Average ads per page: {page_groups['average_group_size']:.1f}")
                        print(f"Median ads per page: {page_groups['median_group_size']:.1f}")
                        print(f"Page with most ads: {page_groups['largest_group']} ads")
                        print(f"Page with fewest ads: {page_groups['smallest_group']} ads")
                        
                        # Show numeric analysis if available
                        if 'numeric_analysis' in page_groups:
                            print(f"\nNumeric columns grouped by page:")
                            for col, stats in page_groups['numeric_analysis'].items():
                                if 'error' not in stats:
                                    print(f"  {col}: Mean varies from {stats['lowest_group_mean']:.2f} to {stats['highest_group_mean']:.2f}")
                    
                    # Try grouping by page_id and ad_id if both exist
                    if 'ad_id' in df.columns:
                        print(f"\n{'Grouping by page_id and ad_id':^60}")
                        print("-" * 60)
                        combo_groups = analyzer.group_analysis(df, ['page_id', 'ad_id'])
                        
                        if 'error' not in combo_groups:
                            print(f"Unique page-ad combinations: {combo_groups['total_groups']}")
                            print(f"Average records per combination: {combo_groups['average_group_size']:.1f}")
                
                # Special analysis for Twitter data
                if dataset_name == 'twitter_posts' and 'user_id' in df.columns:
                    print(f"\n{'Grouping Twitter posts by user_id':^60}")
                    print("-" * 60)
                    user_groups = analyzer.group_analysis(df, ['user_id'])
                    
                    if 'error' not in user_groups:
                        print(f"Total unique users: {user_groups['total_groups']}")
                        print(f"Average posts per user: {user_groups['average_group_size']:.1f}")
                        print(f"Most prolific user: {user_groups['largest_group']} posts")
                        print(f"Least prolific user: {user_groups['smallest_group']} posts")
            else:
                print(f"Error analyzing {dataset_name}: {analysis['error']}")
        else:
            print(f"Failed to load {file_path}")
    
    # Save results to JSON
    if all_results:
        print(f"\n{'='*60}")
        print("Saving results to polars_analysis_results.json...")
        print(f"{'='*60}")
        
        # Make JSON-serializable
        json_results = {}
        for dataset_name, analysis in all_results.items():
            json_results[dataset_name] = {
                'dataset_basics': analysis['dataset_basics'],
                'analysis_time': analysis['analysis_time'],
                'data_quality': analysis['data_quality'],
                'column_summary': {
                    col: {k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v))
                          for k, v in info.items()
                          if k not in ['top_5_frequencies']}  # Exclude complex nested dicts
                    for col, info in analysis['column_analysis'].items()
                }
            }
        
        with open('polars_analysis_results.json', 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        print("Results saved successfully!")
        print(f"Analyzed {len(all_results)} datasets with Polars")
        
        # Performance summary
        total_time = sum(analysis['analysis_time'] for analysis in all_results.values())
        total_rows = sum(analysis['dataset_basics']['num_rows'] for analysis in all_results.values())
        
        print(f"\nPerformance Summary:")
        print(f"Total processing time: {total_time:.2f} seconds")
        print(f"Total rows processed: {total_rows:,}")
        print(f"Average speed: {total_rows/total_time:,.0f} rows/second")
    else:
        print("No datasets were successfully analyzed!")

if __name__ == "__main__":
    run_polars_analysis()