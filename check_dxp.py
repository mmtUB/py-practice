import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kiểm tra và import dnspython
try:
    import dns.resolver
except ImportError:
    print("Lỗi: Thư viện 'dnspython' chưa được cài đặt. Vui lòng chạy: pip install dnspython")
    exit()

def resolve_dns(hostname, record_type):
    """Tra cứu bản ghi DNS cho hostname và record_type (CNAME, A, NS)."""
    try:
        # Tạo resolver với DNS server công cộng và timeout dài hơn
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = ['8.8.8.8', '1.1.1.1']  # Google DNS, Cloudflare DNS
        resolver.timeout = 10  # Timeout 10 giây
        resolver.lifetime = 10
        answers = resolver.resolve(hostname, record_type, raise_on_no_answer=False)
        return [str(rdata) for rdata in answers]
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers):
        return []
    except Exception as e:
        print(f"Lỗi khi tra cứu {record_type} cho {hostname}: {e}")
        return []

def fetch_dns_records(hostname):
    """Lấy CNAME, A, NS cho một hostname."""
    cname_records = resolve_dns(hostname, 'CNAME')
    a_records = resolve_dns(hostname, 'A')
    ns_records = resolve_dns(hostname, 'NS')
    return hostname, cname_records, a_records, ns_records

def add_dns_columns(input_file, output_file, max_workers=10):
    """Đọc file CSV, thêm cột CNAME, A, NS và lưu lại."""
    start_time = time.time()
    try:
        # Đọc file CSV
        df = pd.read_csv(input_file, encoding='utf-8')
        print(f"Đã đọc {len(df)} dòng từ {input_file}")

        # Kiểm tra cột Hostname
        if 'Hostname' not in df.columns:
            print(f"Lỗi: File {input_file} không có cột 'Hostname'.")
            return None

        # Khởi tạo cột DNS
        df['CNAME'] = ''
        df['A'] = ''
        df['NS'] = ''

        # Lấy danh sách hostname duy nhất
        hostnames = df['Hostname'].dropna().unique()

        # Tra cứu DNS song song
        dns_results = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_hostname = {executor.submit(fetch_dns_records, hostname): hostname for hostname in hostnames}
            for future in as_completed(future_to_hostname):
                hostname = future_to_hostname[future]
                try:
                    _, cname, a, ns = future.result()
                    dns_results[hostname] = {
                        'CNAME': ';'.join(cname) if cname else '',
                        'A': ';'.join(a) if a else '',
                        'NS': ';'.join(ns) if ns else ''
                    }
                except Exception as e:
                    print(f"Lỗi khi xử lý {hostname}: {e}")
                    dns_results[hostname] = {'CNAME': '', 'A': '', 'NS': ''}

        # Gán kết quả DNS vào DataFrame
        def apply_dns(row):
            hostname = row['Hostname']
            if pd.notna(hostname) and hostname in dns_results:
                return pd.Series(dns_results[hostname])
            return pd.Series({'CNAME': '', 'A': '', 'NS': ''})

        df[['CNAME', 'A', 'NS']] = df.apply(apply_dns, axis=1)

        # Lưu file
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Đã lưu {len(df)} dòng vào {output_file}")
        print(f"Thời gian xử lý {input_file}: {time.time() - start_time:.2f} giây")

        return df

    except FileNotFoundError:
        print(f"File {input_file} không tồn tại.")
        return None
    except Exception as e:
        print(f"Lỗi khi xử lý {input_file}: {e}")
        return None

def filter_by_dns_conditions(df, dns_conditions, output_file):
    """Lọc dữ liệu dựa trên điều kiện từ cột CNAME, A, NS."""
    try:
        cond = pd.Series([True] * len(df), index=df.index)

        for column, keyword, mode in dns_conditions:
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

        filtered_df = df[cond]
        if filtered_df.empty:
            print("Không có dòng nào thỏa mãn điều kiện lọc DNS.")
            return None

        filtered_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Đã lưu {len(filtered_df)} dòng vào {output_file}")
        return filtered_df

    except Exception as e:
        print(f"Lỗi khi lọc dữ liệu: {e}")
        return None

def process_all_files(input_files, output_folder, dns_conditions, max_workers=10):
    """Xử lý tất cả file CSV: thêm cột DNS và lọc theo điều kiện."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for input_file in input_files:
        file_name = os.path.basename(input_file)
        output_file = os.path.join(output_folder, f"dns_{file_name}")

        # Thêm cột DNS
        df = add_dns_columns(input_file, output_file, max_workers=max_workers)
        if df is None:
            continue

        # Lọc theo điều kiện DNS
        filter_output_file = os.path.join(output_folder, f"filtered_dns_{file_name}")
        filter_by_dns_conditions(df, dns_conditions, filter_output_file)

# Định nghĩa file đầu vào và thư mục đầu ra
input_files = [
    "C:/Users/uyda/OneDrive/Uyen-test/py/output/test_checks.csv",
    "C:/Users/uyda/OneDrive/Uyen-test/py/output/paused_tobe_enabled_checks.csv",
    "C:/Users/uyda/OneDrive/Uyen-test/py/output/everweb_checks.csv"
]
output_folder = "C:/Users/uyda/OneDrive/Uyen-test/py/output/dns_results"

# Điều kiện lọc DNS
dns_conditions = [
    ("CNAME", "dxcloud", "contains"),  # CNAME chứa dxcloud
    ("A", "104.", "contains"),         # A chứa 104.
    ("NS", "episerver", "contains")    # NS chứa episerver
]

# Gọi hàm
process_all_files(input_files, output_folder, dns_conditions=dns_conditions, max_workers=10)