import csv
import math
import json
import ast
from collections import Counter, defaultdict
import time

class BasicDataAnalyzer:
    def __init__(self):
        self.my_data = {}

    def read_csv_file(self, file_name):
        rows = []
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    rows.append(row)
        except FileNotFoundError:
            print(f"Oops! Can't find {file_name}")
            return []
        return rows

    def check_if_number(self, text):
        if not text or text.strip() == '':
            return False
        try:
            float(text)
            return True
        except ValueError:
            return False

    def make_float(self, text):
        try:
            return float(text)
        except (ValueError, TypeError):
            return 0.0

    def get_average(self, numbers):
        if not numbers:
            return 0.0
        return sum(numbers) / len(numbers)

    def get_std_dev(self, numbers):
        if len(numbers) < 2:
            return 0.0
        avg = self.get_average(numbers)
        squared_diffs = [(x - avg) ** 2 for x in numbers]
        variance = sum(squared_diffs) / (len(numbers) - 1)
        return math.sqrt(variance)

    def unpack_delivery_by_region(self, row_val):
        try:
            parsed = ast.literal_eval(row_val)
        except:
            return {'total_spend': 0, 'total_impressions': 0}
        spend, impressions = 0, 0
        for region_data in parsed.values():
            spend += region_data.get('spend', 0)
            impressions += region_data.get('impressions', 0)
        return {'total_spend': spend, 'total_impressions': impressions}

    def unpack_demographics(self, row_val):
        try:
            parsed = ast.literal_eval(row_val)
        except:
            return {'total_spend': 0, 'total_impressions': 0, 'top_demo_by_spend': None}
        spend, impressions = 0, 0
        top_demo = None
        max_spend = 0
        for demo, metrics in parsed.items():
            s = metrics.get('spend', 0)
            i = metrics.get('impressions', 0)
            spend += s
            impressions += i
            if s > max_spend:
                max_spend = s
                top_demo = demo
        return {
            'total_spend': spend,
            'total_impressions': impressions,
            'top_demo_by_spend': top_demo
        }

    def look_at_one_column(self, data_rows, col_name):
        all_values = [row.get(col_name, '') for row in data_rows]
        real_values = [v for v in all_values if v.strip() != '']

        basic_info = {
            'name': col_name,
            'total_rows': len(data_rows),
            'has_data': len(real_values),
            'missing_data': len(data_rows) - len(real_values)
        }

        if col_name == 'delivery_by_region':
            unpacked = [self.unpack_delivery_by_region(val) for val in real_values]
            total_spend = sum([u['total_spend'] for u in unpacked])
            total_impressions = sum([u['total_impressions'] for u in unpacked])
            basic_info.update({
                'data_type': 'nested_dict',
                'total_spend': total_spend,
                'total_impressions': total_impressions
            })
        elif col_name == 'demographic_distribution':
            unpacked = [self.unpack_demographics(val) for val in real_values]
            total_spend = sum([u['total_spend'] for u in unpacked])
            total_impressions = sum([u['total_impressions'] for u in unpacked])
            top_demos = Counter([u['top_demo_by_spend'] for u in unpacked if u['top_demo_by_spend']])
            basic_info.update({
                'data_type': 'nested_dict',
                'total_spend': total_spend,
                'total_impressions': total_impressions,
                'most_targeted_demo': top_demos.most_common(1)
            })
        else:
            number_list = []
            for val in real_values:
                if self.check_if_number(val):
                    number_list.append(self.make_float(val))

            if len(number_list) > 0 and len(number_list) / len(real_values) > 0.5:
                basic_info.update({
                    'data_type': 'numbers',
                    'average': self.get_average(number_list),
                    'smallest': min(number_list) if number_list else None,
                    'biggest': max(number_list) if number_list else None,
                    'spread': self.get_std_dev(number_list)
                })
            else:
                word_counter = Counter(real_values)
                basic_info.update({
                    'data_type': 'text',
                    'different_values': len(word_counter),
                    'most_common': word_counter.most_common(5),
                    'sample_values': list(word_counter.keys())[:10]
                })

        return basic_info

    def analyze_whole_dataset(self, data_rows, name):
        if not data_rows:
            return {'error': 'No data found!'}

        start = time.time()

        dataset_info = {
            'name': name,
            'num_rows': len(data_rows),
            'num_columns': len(data_rows[0].keys()) if data_rows else 0,
            'column_names': list(data_rows[0].keys()) if data_rows else []
        }

        column_info = {}
        for col in dataset_info['column_names']:
            column_info[col] = self.look_at_one_column(data_rows, col)

        end = time.time()

        return {
            'dataset_info': dataset_info,
            'column_info': column_info,
            'time_taken': end - start
        }

    def group_data_by(self, data_rows, group_by_cols):
        if not data_rows or not group_by_cols:
            return {}

        my_groups = defaultdict(list)
        for row in data_rows:
            key = tuple(row.get(col, '') for col in group_by_cols)
            my_groups[key].append(row)

        group_results = {}
        for key, group_rows in my_groups.items():
            key_name = '_'.join(str(k) for k in key)
            group_results[key_name] = {
                'size': len(group_rows),
                'values': dict(zip(group_by_cols, key))
            }

        return {
            'total_groups': len(my_groups),
            'group_details': group_results,
            'avg_group_size': sum(len(g) for g in my_groups.values()) / len(my_groups)
        }

    def show_results(self, results):
        print(f"\n{'='*50}")
        print(f"ANALYZING: {results['dataset_info']['name']}")
        print(f"{'='*50}")
        print(f"Rows: {results['dataset_info']['num_rows']:,}")
        print(f"Columns: {results['dataset_info']['num_columns']}")
        print(f"Time: {results['time_taken']:.2f} seconds")

        print(f"\n{'Looking at each column':^50}")
        print("-" * 50)

        for col_name, info in results['column_info'].items():
            print(f"\nColumn: {col_name}")
            print(f"  Type: {info['data_type']}")
            print(f"  Total: {info['total_rows']:,}")
            print(f"  Has data: {info['has_data']:,}")
            print(f"  Missing: {info['missing_data']:,}")

            if info['data_type'] == 'numbers':
                print(f"  Average: {info['average']:.2f}")
                print(f"  Min: {info['smallest']}")
                print(f"  Max: {info['biggest']}")
                print(f"  Spread: {info['spread']:.2f}")
            elif info['data_type'] == 'nested_dict':
                print(f"  Total Spend: {info['total_spend']}")
                print(f"  Total Impressions: {info['total_impressions']}")
                if 'most_targeted_demo' in info:
                    print(f"  Most Targeted Demo: {info['most_targeted_demo']}")
            else:
                print(f"  Different values: {info['different_values']:,}")
                print(f"  Most common: {info['most_common'][:3]}")

def main():
    my_analyzer = BasicDataAnalyzer()

    my_files = {
        'facebook_ads': '2024_fb_ads_president_scored_anon.csv',
        'facebook_posts':'2024_fb_posts_president_scored_anon.csv', 
        'twitter_posts':'2024_tw_posts_president_scored_anon.csv'
    }

    all_results = {}

    for dataset_name, filename in my_files.items():
        print(f"\nReading {filename}...")
        my_data = my_analyzer.read_csv_file(filename)

        if my_data:
            results = my_analyzer.analyze_whole_dataset(my_data, dataset_name)
            all_results[dataset_name] = results
            my_analyzer.show_results(results)

            if dataset_name == 'facebook_ads':
                print(f"\n{'Grouping by page_id':^50}")
                print("-" * 50)
                page_groups = my_analyzer.group_data_by(my_data, ['page_id'])
                print(f"Number of pages: {page_groups.get('total_groups', 0)}")
                print(f"Average ads per page: {page_groups.get('avg_group_size', 0):.1f}")

                print(f"\n{'Grouping by page_id and ad_id':^50}")
                print("-" * 50)
                ad_groups = my_analyzer.group_data_by(my_data, ['page_id', 'ad_id'])
                print(f"Unique page-ad combos: {ad_groups.get('total_groups', 0)}")
                print(f"Average size: {ad_groups.get('avg_group_size', 0):.1f}")

    with open('basic_python_results.json', 'w') as f:
        json_data = {}
        for dataset, results in all_results.items():
            json_data[dataset] = {
                'dataset_info': results['dataset_info'],
                'time_taken': results['time_taken'],
                'column_summary': {
                    col: {k: v for k, v in info.items() 
                          if k not in ['sample_values', 'most_common']}
                    for col, info in results['column_info'].items()
                }
            }
        json.dump(json_data, f, indent=2)

    print(f"\n{'='*50}")
    print("All done! Results saved to basic_python_results.json")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
