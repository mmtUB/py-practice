count = 0 
for i in range(1,101):
    if i % 7 == 0:
        count += 1
        print(i, end=" ")
print(f"\nTong cong có {count} so chia het cho 7.")


n = int(input("Nhap so dong: "))
for i in range(1, n + 1):
    print("*" * i)