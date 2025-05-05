print("Nhap tuoi")
tuoi = int(input())
if tuoi <= 13 :
    print("Ban la tre em")
elif 13 < tuoi <= 18 :
    print("Ban la thieu nien")
else :
    print("Ban la nguoi lon")


for i in range(1,11):
    print(f"5 x {i} = {5 * i}")

count = 0
for i in range(1,101):
    if i % 2 == 0 :
          count += 1
print(f"Có {count} số chẵn")


