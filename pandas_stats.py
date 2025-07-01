import pandas as pd
import numpy as np
import json
import time
import ast

class PandasHelper:
    def __init__(self):
        self.my_datasets = {}

    def parse_if_json_like(self, val):
        """Try to parse text like a dictionary (e.g., delivery_by_region)"""
        try:
            return ast.literal_eval(val)
        except:
            return val

    def load_my_csv(self, file_path):
        """Load CSV with pandas and parse JSON-like columns"""
        try:
            df = pd.read_csv(file_path)
            for col in df.columns:
                if df[col].dtype == 'object' and df[col].str.startswith('{').any():
                    df[col] = df[col].apply(lambda x: self.parse_if_json_like(x) if isinstance(x, str) else x)
            print(f"Loaded {len(df)} rows and {len(df.columns)} columns")
            return df
        except FileNotFoundError:
            print(f"Can't find file: {file_path}")
            return pd.DataFrame()
        except Exception as error:
            print(f"Something went wrong: {error}")
            return pd.DataFrame()

    def examine_dataset(self, df, dataset_name):
        if df.empty:
            return {'error': 'Dataset is empty!'}

        timer_start = time.time()

        basic_stuff = {
            'dataset_name': dataset_name,
            'row_count': len(df),
            'col_count': len(df.columns),
            'memory_size': df.memory_usage(deep=True).sum(),
            'all_columns': df.columns.tolist()
        }

        col_details = {}

        for col in df.columns:
            current_col = df[col]

            col_stats = {
                'column_name': col,
                'data_type': str(current_col.dtype),
                'total_count': len(current_col),
                'not_empty': current_col.count(),
                'empty_count': current_col.isnull().sum(),
                'percent_missing': (current_col.isnull().sum() / len(current_col)) * 100
            }

            if pd.api.types.is_numeric_dtype(current_col):
                num_summary = current_col.describe()
                def get_safe(summary, key):
                    return summary[key] if key in summary else None

                col_stats.update({
                    'type': 'numeric',
                    'mean_value': get_safe(num_summary, 'mean'),
                    'std_dev': get_safe(num_summary, 'std'),
                    'min_value': get_safe(num_summary, 'min'),
                    'max_value': get_safe(num_summary, 'max'),
                    'q1': get_safe(num_summary, '25%'),
                    'median': get_safe(num_summary, '50%'),
                    'q3': get_safe(num_summary, '75%'),
                    'skew': current_col.skew(),
                    'kurt': current_col.kurtosis()
                })

            elif current_col.apply(lambda x: isinstance(x, dict)).any():
                sample = current_col.dropna().iloc[0]
                col_stats.update({
                    'type': 'dict-like',
                    'sample_keys': list(sample.keys()) if isinstance(sample, dict) else [],
                    'example': sample
                })

            else:
                freq_counts = current_col.value_counts()
                col_stats.update({
                    'type': 'categorical',
                    'unique_vals': current_col.nunique(),
                    'top_value': freq_counts.index[0] if len(freq_counts) > 0 else None,
                    'top_count': freq_counts.iloc[0] if len(freq_counts) > 0 else 0,
                    'freq_top5': freq_counts.head().to_dict()
                })

            col_details[col] = col_stats

        total_cells = len(df) * len(df.columns)
        missing_cells = df.isnull().sum().sum()

        quality_check = {
            'data_completeness': ((total_cells - missing_cells) / total_cells) * 100,
            'cols_with_missing': df.isnull().any().sum(),
            'total_missing': missing_cells
        }

        timer_end = time.time()

        return {
            'basic_stuff': basic_stuff,
            'col_details': col_details,
            'quality_check': quality_check,
            'processing_time': timer_end - timer_start
        }

    def do_groupby_stuff(self, df, group_columns):
        if df.empty or not group_columns:
            return {}

        try:
            missing = [col for col in group_columns if col not in df.columns]
            if missing:
                return {'error': f'These columns are missing: {missing}'}

            grouped_data = df.groupby(group_columns)

            group_info = {
                'num_groups': grouped_data.ngroups,
                'group_size_stats': grouped_data.size().describe().to_dict(),
                'smallest_group': grouped_data.size().min(),
                'largest_group': grouped_data.size().max(),
                'average_group_size': grouped_data.size().mean()
            }

            number_cols = df.select_dtypes(include=[np.number]).columns
            number_group_analysis = {}
            for num_col in number_cols:
                if num_col not in group_columns:
                    try:
                        group_stats = grouped_data[num_col].agg(['count', 'mean', 'std', 'min', 'max'])
                        number_group_analysis[num_col] = {
                            'avg_of_means': group_stats['mean'].mean(),
                            'std_of_means': group_stats['mean'].std(),
                            'lowest_group_mean': group_stats['mean'].min(),
                            'highest_group_mean': group_stats['mean'].max()
                        }
                    except:
                        continue

            group_info['number_analysis'] = number_group_analysis

            return group_info

        except Exception as e:
            return {'error': f'Groupby failed: {str(e)}'}

    def print_my_results(self, analysis):
        print(f"\n{'='*55}")
        print(f"PANDAS RESULTS: {analysis['basic_stuff']['dataset_name']}")
        print(f"{'='*55}")
        print(f"Rows: {analysis['basic_stuff']['row_count']:,}")
        print(f"Columns: {analysis['basic_stuff']['col_count']}")
        print(f"Memory: {analysis['basic_stuff']['memory_size']:,} bytes")
        print(f"Time to analyze: {analysis['processing_time']:.2f} seconds")
        print(f"Data quality: {analysis['quality_check']['data_completeness']:.1f}% complete")

        print(f"\n{'Column Summary':^55}")
        print("-" * 55)

        numeric_list = []
        text_list = []

        for col_name, details in analysis['col_details'].items():
            if details['type'] == 'numeric':
                numeric_list.append(col_name)
            else:
                text_list.append(col_name)

        print(f"Number columns: {len(numeric_list)}")
        print(f"Text columns: {len(text_list)}")

        missing_data = [(col, details['percent_missing']) 
                        for col, details in analysis['col_details'].items()]
        missing_data.sort(key=lambda x: x[1], reverse=True)

        print(f"\nColumns with missing data:")
        for col, pct in missing_data[:5]:
            if pct > 0:
                print(f"  {col}: {pct:.1f}% missing")

        print(f"\nSample numeric columns:")
        for col_name in numeric_list[:3]:
            details = analysis['col_details'][col_name]
            print(f"\n{col_name}:")
            print(f"  Average: {details['mean_value']:.2f}")
            print(f"  Std Dev: {details['std_dev']:.2f}")
            print(f"  Range: {details['min_value']} to {details['max_value']}")

        print(f"\nSample text columns:")
        for col_name in text_list[:3]:
            details = analysis['col_details'][col_name]
            print(f"\n{col_name}:")
            print(f"  Unique values: {details.get('unique_vals', '?'):,}")
            print(f"  Most common: {details.get('top_value')} ({details.get('top_count')} times)")

def run_analysis():
    helper = PandasHelper()

    my_files = {
        'fb_ads_data': '2024_fb_ads_president_scored_anon.csv',
        'fb_posts_data': '2024_fb_posts_president_scored_anon.csv',
        'twitter_data': '2024_tw_posts_president_scored_anon.csv'
    }

    all_my_results = {}

    for name, filename in my_files.items():
        print(f"\nLoading {filename} with pandas...")
        my_df = helper.load_my_csv(filename)

        if not my_df.empty:
            results = helper.examine_dataset(my_df, name)
            all_my_results[name] = results
            helper.print_my_results(results)

            if name == 'fb_ads_data' and 'page_id' in my_df.columns:
                print(f"\n{'Grouping by page_id':^55}")
                print("-" * 55)
                page_analysis = helper.do_groupby_stuff(my_df, ['page_id'])
                if 'error' not in page_analysis:
                    print(f"Number of different pages: {page_analysis['num_groups']}")
                    print(f"Average ads per page: {page_analysis['average_group_size']:.1f}")

                if 'ad_id' in my_df.columns:
                    print(f"\n{'Grouping by page_id and ad_id':^55}")
                    print("-" * 55)
                    combo_analysis = helper.do_groupby_stuff(my_df, ['page_id', 'ad_id'])
                    if 'error' not in combo_analysis:
                        print(f"Unique page-ad combinations: {combo_analysis['num_groups']}")
                        print(f"Average records per combo: {combo_analysis['average_group_size']:.1f}")

    with open('pandas_student_results.json', 'w') as output_file:
        json_friendly = {}
        for dataset, analysis in all_my_results.items():
            json_friendly[dataset] = {
                'basic_stuff': analysis['basic_stuff'],
                'processing_time': analysis['processing_time'],
                'quality_check': analysis['quality_check'],
                'column_summary': {
                    col: {
                        k: (
                            v if pd.api.types.is_scalar(v) and not pd.isna(v)
                            else str(v)
                        )
                        for k, v in details.items()
                        if k != 'freq_top5'
                    }
                    for col, details in analysis['col_details'].items()
                }
            }
        json.dump(json_friendly, output_file, indent=2, default=str)

    print(f"\n{'='*55}")
    print("Pandas analysis finished! Saved to pandas_student_results.json")
    print(f"{'='*55}")

if __name__ == "__main__":
    run_analysis()
