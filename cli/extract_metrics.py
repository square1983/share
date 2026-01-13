import json
import sys

def extract_metrics(file_path):
    """
    指定されたJSONファイルからLambda Insightsのメトリクスを抽出する
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"エラー: ファイル '{file_path}' が見つかりません。")
        return
    except json.JSONDecodeError:
        print(f"エラー: '{file_path}' は有効なJSONではありません。")
        return

    extracted_data = []

    # 'events' 配列をループ
    if 'events' in data:
        for event in data['events']:
            if 'message' in event:
                try:
                    # 'message' フィールド自体がJSON文字列になっているため、再度パースする
                    message_str = event['message']
                    message_data = json.loads(message_str)

                    # 必要なフィールドを抽出
                    metrics = {
                        'cpu_total_time': message_data.get('cpu_total_time'),
                        'memory_utilization': message_data.get('memory_utilization'),
                        'duration': message_data.get('duration')
                    }
                    extracted_data.append(metrics)

                except json.JSONDecodeError:
                    # messageがJSONでない場合はスキップ
                    continue
    
    return extracted_data

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_metrics.py <json_file_path>")
        sys.exit(1)

    input_file = sys.argv[1]
    results = extract_metrics(input_file)

    if results:
        for i, res in enumerate(results):
            print(f"--- Event {i+1} ---")
            print(f"CPU Total Time: {res['cpu_total_time']}")
            print(f"Memory Utilization: {res['memory_utilization']}")
            print(f"Duration: {res['duration']}")
    else:
        print("指定されたフィールドを含むメトリクスが見つかりませんでした。")
