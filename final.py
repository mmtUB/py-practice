import pandas as pd
import os
import time
import json

def filter_by_condition(df, conditions):
    try:
        # Khởi tạo điều kiện True cho tất cả dòng
        cond = pd.Series([True] * len(df), index=df.index)

        for condition in conditions:
            # Kiểm tra số phần tử trong condition
            if len(condition) != 3:
                print(f"Lỗi: Điều kiện {condition} không có đúng 3 phần tử (column, keyword, mode).")
                continue
            column, keyword, mode = condition

            if column not in df.columns:
                print(f"Cột {column} không tồn tại trong DataFrame.")
                continue

            if mode == "contains":
                cond &= df[column].astype(str).str.contains(keyword, case=False, na=False, regex=True)
            elif mode == "not_contains":
                cond &= ~df[column].astype(str).str.contains(keyword, case=False, na=False, regex=True)
            elif mode == "is_empty":
                cond &= df[column].isna() | (df[column].astype(str).str.strip() == "")
            elif mode == "not_empty":
                cond &= ~(df[column].isna() | (df[column].astype(str).str.strip() == ""))
            else:
                print(f"Mode {mode} không được hỗ trợ.")

        return df[cond], df[~cond]
    except Exception as e:
        print(f"Lỗi trong filter_by_condition: {e}")
        return df[[]], df  # Trả về DataFrame rỗng và nguyên bản nếu lỗi

def process_csv_sequential(input_file, output_folder, condition_groups, duplicate_column='Hostname', save_remainder=True):
    start_time = time.time()
    try:
        # Đọc file CSV
        print("Đang đọc file CSV...")
        df = pd.read_csv(input_file, encoding='utf-8')
        print(f"Đã đọc {len(df)} dòng dữ liệu.")

        # Lọc duplicates dựa trên cột Hostname
        initial_len = len(df)
        # Giữ dòng đầu tiên, loại các dòng trùng lặp
        df_unique = df.drop_duplicates(subset=[duplicate_column], keep='first')
        # Tìm các dòng bị trùng lặp (không bao gồm dòng đầu tiên được giữ)
        df_duplicates = df[df.duplicated(subset=[duplicate_column], keep='first')]
        print(f"Đã loại {initial_len - len(df_unique)} dòng trùng lặp dựa trên cột {duplicate_column}.")

        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Lưu các dòng trùng lặp vào file riêng
        if not df_duplicates.empty:
            duplicates_file = os.path.join(output_folder, "duplicates.csv")
            df_duplicates.to_csv(duplicates_file, index=False, encoding='utf-8')
            print(f"Đã lưu {len(df_duplicates)} dòng trùng lặp vào {duplicates_file}")
        else:
            print("Không có dòng trùng lặp nào.")

        # Khởi tạo remaining_df từ dữ liệu đã loại duplicates
        remaining_df = df_unique.copy()

        # Duyệt qua từng nhóm điều kiện
        total_filtered = 0
        for group_name, group_conditions in condition_groups.items():
            print(f"\nXử lý nhóm: {group_name}")
            df_group, remaining_df = filter_by_condition(remaining_df, group_conditions)
            
            # Lưu kết quả nhóm
            output_file = os.path.join(output_folder, f"{group_name}_checks.csv")
            df_group.to_csv(output_file, index=False, encoding='utf-8')
            print(f"Đã lưu {len(df_group)} dòng vào {output_file}")
            total_filtered += len(df_group)

        # Lưu remainder nếu yêu cầu
        if save_remainder and not remaining_df.empty:
            remainder_file = os.path.join(output_folder, "remainings.csv")
            remaining_df.to_csv(remainder_file, index=False, encoding='utf-8')
            print(f"Đã lưu {len(remaining_df)} dòng vào {remainder_file}")
        else:
            print("Không có dữ liệu còn lại hoặc không yêu cầu lưu remainder.")

        print(f"\nTổng dòng lọc được: {total_filtered}")
        print(f"Tổng dòng còn lại: {len(remaining_df)}")
        print(f"Thời gian xử lý: {time.time() - start_time:.2f} giây")
        
    except FileNotFoundError:
        print(f"File {input_file} không tồn tại.")
    except Exception as e:
        print(f"Lỗi: {e}")

# Cấu hình điều kiện
condition_groups = {
    "paused_tobe_enable": [
        ("Status", "paused", "contains"),
        ("Tags", "social|emea|perform|personalization|endpoint|paas|paasportal|opti|gateway|checkid|cmp|b2b", "not_contains"),
        ("Last Check Time (UTC)", "", "is_empty"),
        ("Hostname", "dxcloud|prod", "not_contains")
    ],
    "test": [
        ("Hostname", "inte.|int.|integration|prep|prod.|prod-|production|beta|alpha|stage|stg|staging|ade.|test|uat|dev.|qa|cms|preview|prd|portal|intranet", "contains"),
        ("Hostname", "dxcloud|www", "not_contains"),
        ("Tags", "social|emea|perform|personalization|endpoint|paas|paasportal|opti|gateway|checkid|cmp|b2b", "not_contains")
    ],
    "everweb": [
        ("Tags", "dxc|dxp|service|production|epi|social|emea|perform|personalization|endpoint|paas|paasportal|opti|gateway|checkid|cmp|b2b", "not_contains")
    ]
}

# Gọi hàm
input_file = "C:/Users/your_name/input.csv"
output_folder = "C:/Users/your_name/output"
duplicate_column = 'Hostname'  # Lọc duplicates dựa trên cột Hostname
process_csv_sequential(input_file, output_folder, condition_groups, duplicate_column=duplicate_column, save_remainder=True)